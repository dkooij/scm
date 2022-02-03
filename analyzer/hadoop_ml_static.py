"""
Train ML models to predict page text changes using static features.
Author: Daan Kooij
Last modified: February 3rd, 2022
"""

import hashlib
from pyspark import SparkContext
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.linalg import DenseVector
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql import Row, SparkSession
import time


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SCM-ML-STATIC")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def setup(input_path):
    df = spark.read.csv(input_path).repartition(600)
    df = df.rdd.map(lambda row: Row(**{"page_id": row[0],
                                       "features": DenseVector([int(x) for x in row[1:10]]),
                                       "label": int(row[10])})).toDF()

    count_zero, count_one = df.filter(df.label == 0).count(), df.filter(df.label == 1).count()
    weight_zero, weight_one = count_one / count_zero, 1.0  # Assuming that count_zero >= count_one
    df = df.rdd.map(lambda row: Row(**{"features": row["features"],
                                       "label": row["label"],
                                       "weight": weight_zero if row["label"] == 0 else weight_one,
                                       "validation": int(hashlib.md5(
                                           row["page_id"].encode("utf-8")).hexdigest(), base=16) % 5 == 4,
                                       })).toDF()

    (data_train, data_test) = df.filter(~df.validation), df.filter(df.validation)

    data_train_balanced_zero = data_train.filter(data_train.label == 0).sample(weight_zero, 42)
    data_train_balanced_one = data_train.filter(data_train.label == 1)
    data_train_balanced = data_train_balanced_zero.union(data_train_balanced_one)

    return data_train, data_test, data_train_balanced


def save_checkpoint(df, name):
    df.rdd.saveAsPickleFile("checkpoints/" + name + ".pickle")


def load_checkpoint(name):
    rdd = sc.pickleFile("checkpoints/" + name + ".pickle")
    return rdd.toDF()


def create_dataframes(input_path, checkpoint_name):
    data_train, data_test, data_train_balanced = setup(input_path)
    save_checkpoint(data_train, checkpoint_name + "-train")
    save_checkpoint(data_train_balanced, checkpoint_name + "-train-balanced")
    save_checkpoint(data_test, checkpoint_name + "-test")


def load_dataframes(checkpoint_name):
    return load_checkpoint(checkpoint_name + "-train-balanced"), load_checkpoint(checkpoint_name + "-test")


def select_feature_subset(df, feature_subset):
    return df.rdd.map(lambda row: Row(**{"features": DenseVector([row["features"][i] for i in feature_subset]),
                                         "label": row["label"],
                                         "weight": row["weight"],
                                         "validation": row["validation"],
                                         })).toDF()


def train_random_forest(data_train, num_trees=20, max_depth=10, min_instances_per_node=1, num_folds=5):
    rf = RandomForestClassifier(numTrees=num_trees, maxDepth=max_depth,
                                minInstancesPerNode=min_instances_per_node, seed=42)

    # count = int(data_train.count() * (num_folds - 1) / num_folds)
    # param_grid = ParamGridBuilder() \
    #     .addGrid(rf.numTrees, [50]) \
    #     .addGrid(rf.maxDepth, [20]) \
    #     .addGrid(rf.minInstancesPerNode, [1]) \
    #     .build()
    # param_grid = ParamGridBuilder().addGrid(rf.maxDepth, [2]).build()
    # cv = CrossValidator(estimator=rf, estimatorParamMaps=param_grid,
    #                     evaluator=BinaryClassificationEvaluator(), numFolds=num_folds, seed=42)

    # cv = cv.fit(data_train)
    # best_model = cv.bestModel
    best_model = rf.fit(data_train)

    print("Random Forest parameters:")
    # print("- Number of folds (cv):", num_folds)
    print("- Number of trees:", best_model.getOrDefault("numTrees"))
    print("- Maximum tree depth:", best_model.getOrDefault("maxDepth"))
    print("- Minimum instances per node:", best_model.getOrDefault("minInstancesPerNode"))
    # print("- Number of features used:", len(data_train.first()["features"]))

    return best_model


def train_logistic_regression(data_train, num_folds=5):
    lr = LogisticRegression()

    param_grid = ParamGridBuilder() \
        .addGrid(lr.family, ["binomial", "multinomial"]) \
        .addGrid(lr.elasticNetParam, [0.0, 1.0]) \
        .addGrid(lr.regParam, [0.01, 0.1, 1, 10, 100]) \
        .build()
    cv = CrossValidator(estimator=lr, estimatorParamMaps=param_grid,
                        evaluator=BinaryClassificationEvaluator(), numFolds=num_folds, seed=42)

    cv = cv.fit(data_train)
    best_model = cv.bestModel
    # best_model = lr.fit(data_train)

    print("Logistic Regression parameters:")
    print("- Number of folds (cv):", num_folds)
    print("- Solver:", best_model.getOrDefault("family"))
    print("- Regularization penalty:", best_model.getOrDefault("elasticNetParam"))
    print("- Regularization penalty strength:", best_model.getOrDefault("regParam"))
    # print("- Number of features used:", len(data_train.first()["features"]))

    return best_model


def evaluate(trained_model, data_test, model_type):
    predictions = trained_model.transform(data_test)

    predictions_rdd = predictions.select("label", "prediction").rdd.map(
        lambda row: (float(row["label"]), float(row["prediction"])))

    metrics = MulticlassMetrics(predictions_rdd)
    confusion_matrix = [[int(v) for v in inner_list] for inner_list in metrics.confusionMatrix().toArray()]

    recall_zero = confusion_matrix[0][0] / (confusion_matrix[0][0] + confusion_matrix[1][0])
    recall_one = confusion_matrix[1][1] / (confusion_matrix[0][1] + confusion_matrix[1][1])
    recall_minimum = min(recall_zero, recall_one)

    print("\nMetrics:")
    print("- Confusion matrix:", confusion_matrix)
    print("- Minimum recall:", str(round(recall_minimum * 100, 2)) + "%")

    if model_type == "rf":
        print("\nFeature importances:")
        print(trained_model.featureImportances)
    elif model_type == "lr":
        print("\nModel coefficients:")
        print(trained_model.coefficients)


def train_evaluate(model_type, model_name):
    # create_dataframes("extracted/static-training-pairs-combined-2.csv", "data")
    data_train_balanced, data_test = load_dataframes("data")

    start_time = time.time()
    if model_type == "rf":
        trained_model = train_random_forest(data_train_balanced, num_trees=50,
                                            max_depth=15, min_instances_per_node=100)
    else:
        trained_model = train_logistic_regression(data_train_balanced)
    trained_model.save("models/" + model_name + ".model")
    evaluate(trained_model, data_test, model_type)
    execution_time = int(time.time() - start_time)
    print("\nExecution time:", execution_time, "seconds")
    print("\n--------------------------------\n")


train_evaluate("lr", "lr")
