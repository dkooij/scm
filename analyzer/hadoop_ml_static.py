"""
Train ML models to predict page text changes using static features.
Author: Daan Kooij
Last modified: January 20th, 2022
"""

import hashlib
from pyspark import SparkContext
from pyspark.ml.classification import DecisionTreeClassifier, LinearSVC, LogisticRegression, NaiveBayes, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.linalg import DenseVector
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql import Row, SparkSession


INPUT_PATH = "extracted/static-training-pairs-combined-2-sample.csv"


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SCM-EXTRACT-STATIC-TRAINING-PAIRS")
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


def train_tune_random_forest(rf, data_train):
    param_grid = ParamGridBuilder() \
        .addGrid(rf.impurity, ["gini", "entropy"]) \
        .addGrid(rf.maxBins, [16, 32]) \
        .build()
    tv_split = TrainValidationSplit(estimator=rf,
                                    estimatorParamMaps=param_grid,
                                    evaluator=BinaryClassificationEvaluator(),
                                    trainRatio=0.8,
                                    seed=42)
    return tv_split.fit(data_train)


def evaluate(trained_model, data_test, model_type, model_setting):
    predictions = trained_model.transform(data_test)

    predictions_rdd = predictions.select("label", "prediction").rdd.map(
        lambda row: (float(row["label"]), float(row["prediction"])))

    metrics = MulticlassMetrics(predictions_rdd)
    confusion_matrix = [[int(v) for v in inner_list] for inner_list in metrics.confusionMatrix().toArray()]

    print("Model type: " + model_type + ", setting: " + model_setting)
    print(str(confusion_matrix[0]) + "\n" + str(confusion_matrix[1]) + "\n")


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
            rf_model = RandomForestClassifier(labelCol="label", featuresCol="features", seed=42)
            # rf_model = RandomForestClassifier(labelCol="label", featuresCol="features",
            #                                   minInstancesPerNode=int(0.0001 * data_train_balanced.count()))
            if "standard" in model_settings:
                yield train_tune_random_forest(rf_model, data_train), "rf", "standard"
            if "balanced" in model_settings:
                yield train_tune_random_forest(rf_model, data_train_balanced), "rf", "balanced"


_model_types = ("rf",)
_model_settings = ("balanced",)

# _data_train, _data_test, _data_train_balanced = setup()
# save_checkpoint(_data_train, "data-train")
# save_checkpoint(_data_train_balanced, "data-train-balanced")
# save_checkpoint(_data_test, "data-test")

_data_train_balanced, _data_test = load_checkpoint("sample-train-balanced"), load_checkpoint("sample-test")

_trained_models = train_models(None, _data_train_balanced, _model_types, _model_settings)
for _trained_model, _model_type, _model_setting in _trained_models:
    # print("MODEL PARAMETERS:", _trained_model.explainParams(), "\n")
    # print("FEATURE IMPORTANCES:", _trained_model.featureImportances, "\n")
    evaluate(_trained_model, _data_test, _model_type, _model_setting)
