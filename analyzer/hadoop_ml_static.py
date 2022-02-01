"""
Train ML models to predict page text changes using static features.
Author: Daan Kooij
Last modified: February 1st, 2022
"""

import hashlib
from pyspark import SparkContext
from pyspark.ml.classification import DecisionTreeClassifier, LinearSVC, LogisticRegression, NaiveBayes, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.linalg import DenseVector
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql import Row, SparkSession
import time


INPUT_PATH = "extracted/static-training-pairs-combined-2.csv"


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SCM-ML-STATIC")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def setup():
    df = spark.read.csv(INPUT_PATH).repartition(600)
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
    print("- Number of features used:", len(data_train.first()["features"]))

    return best_model


def select_feature_subset(df, feature_subset):
    return df.rdd.map(lambda row: Row(**{"features": DenseVector([row["features"][i] for i in feature_subset]),
                                         "label": row["label"],
                                         "weight": row["weight"],
                                         "validation": row["validation"],
                                         })).toDF()


"""
def train_random_forests(data_train, depth_range):
    for d in depth_range:
        yield train_random_forest(data_train, max_depth=d)
"""


def evaluate(trained_model, data_test):
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

    print("\nFeature importances:")
    print(trained_model.featureImportances)


"""
def train_models(data_train, data_train_balanced, model_types, model_settings):
    for model_type in model_types:
        if model_type == "lr":
            lr_model_standard = LogisticRegression(labelCol="label", featuresCol="features")
            if "standard" in model_settings:
                yield lr_model_standard.fit(data_train), "lr", "standard"
            if "weighted" in model_settings:
                lr_model_weighted = LogisticRegression(labelCol="label", featuresCol="features", weightCol="weight")
                yield lr_model_weighted.fit(data_train), "lr", "weighted"
            if "balanced" in model_settings:
                yield lr_model_standard.fit(data_train_balanced), "lr", "balanced"
        elif model_type == "svm":
            svm_model_standard = LinearSVC(labelCol="label", featuresCol="features")
            if "standard" in model_settings:
                yield svm_model_standard.fit(data_train), "svm", "standard"
            if "weighted" in model_settings:
                svm_model_weighted = LinearSVC(labelCol="label", featuresCol="features", weightCol="weight")
                yield svm_model_weighted.fit(data_train), "svm", "weighted"
            if "balanced" in model_settings:
                yield svm_model_standard.fit(data_train_balanced), "svm", "balanced"
        elif model_type == "nb":
            nb_model_standard = NaiveBayes(labelCol="label", featuresCol="features")
            if "standard" in model_settings:
                yield nb_model_standard.fit(data_train), "nb", "standard"
            if "weighted" in model_settings:
                nb_model_weighted = NaiveBayes(labelCol="label", featuresCol="features", weightCol="weight")
                yield nb_model_weighted.fit(data_train), "nb", "weighted"
            if "balanced" in model_settings:
                yield nb_model_standard.fit(data_train_balanced), "nb", "balanced"
        elif model_type == "dt":
            dt_model = DecisionTreeClassifier(labelCol="label", featuresCol="features")
            if "standard" in model_settings:
                yield dt_model.fit(data_train), "dt", "standard"
            if "balanced" in model_settings:
                yield dt_model.fit(data_train_balanced), "dt", "balanced"
        else:  # model_type == "rf"
            yield train_random_forest(data_train_balanced)
"""


# _data_train, _data_test, _data_train_balanced = setup()
# save_checkpoint(_data_train, "data-train")
# save_checkpoint(_data_train_balanced, "data-train-balanced")
# save_checkpoint(_data_test, "data-test")

_data_train_balanced, _data_test = load_checkpoint("data-train-balanced"), load_checkpoint("data-test")

# _trained_models = train_random_forests(_data_train_balanced, range(1, 21))
_feature_subsets = [(0, 1, 2, 3, 4, 5, 6, 7, 8),
                    (0, 1, 2, 3, 4, 5, 6, 8),
                    (1, 2, 3, 4, 5, 6, 8),
                    (1, 2, 3, 5, 6, 8),
                    (2, 3, 5, 6, 8),
                    (3, 5, 6, 8),
                    (3, 5, 8),
                    (5, 8),
                    (8,)]

for _fs in _feature_subsets[:1]:
    start_time = time.time()
    _data_train_balanced_fs = select_feature_subset(_data_train_balanced, _fs)
    _data_test_fs = select_feature_subset(_data_test, _fs)
    _trained_model = train_random_forest(_data_train_balanced_fs, num_trees=50,
                                         max_depth=15, min_instances_per_node=100)
    _trained_model.save("models/rf.model")
    evaluate(_trained_model, _data_test_fs)
    execution_time = int(time.time() - start_time)
    print("\nExecution time:", execution_time, "seconds")
    print("\n--------------------------------\n")
