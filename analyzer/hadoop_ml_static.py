"""
Train ML models to predict page text changes using static features.
Author: Daan Kooij
Last modified: January 12th, 2021
"""

from pyspark import SparkContext
from pyspark.ml.classification import DecisionTreeClassifier, LinearSVC, LogisticRegression, NaiveBayes, RandomForestClassifier
from pyspark.ml.linalg import DenseVector
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql import Row, SparkSession


INPUT_PATH = "extracted/static-training-pairs-combined.csv"


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SCM-EXTRACT-STATIC-TRAINING-PAIRS")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def setup():
    df = spark.read.csv(INPUT_PATH).repartition(600)
    df = df.rdd.map(lambda row: Row(**{"features": DenseVector([int(x) for x in row[:9]]),
                                       "target": int(row[9])})).toDF()
    count_zero, count_one = df.filter(df.target == 0).count(), df.filter(df.target == 1).count()
    weight_zero, weight_one = count_one / count_zero, 1.0  # Assuming that count_zero >= count_one
    df = df.rdd.map(lambda row: Row(**{"features": row["features"],
                                       "target": row["target"],
                                       "weight": weight_zero if row["target"] == 0 else weight_one})).toDF()
    (data_train, data_test) = df.randomSplit([0.8, 0.2], seed=42)

    data_train_balanced_zero = data_train.filter(data_train.target == 0).sample(weight_zero, 42)
    data_train_balanced_one = data_train.filter(data_train.target == 1)
    data_train_balanced = data_train_balanced_zero.union(data_train_balanced_one)

    return data_train, data_test, data_train_balanced


def evaluate(trained_model, data_test, model_type, model_setting):
    predictions = trained_model.transform(data_test)

    predictions_rdd = predictions.select("target", "prediction").rdd.map(
        lambda row: (float(row["target"]), float(row["prediction"])))

    metrics = MulticlassMetrics(predictions_rdd)
    confusion_matrix = [[int(v) for v in inner_list] for inner_list in metrics.confusionMatrix().toArray()]

    print("Model type: " + model_type + ", setting: " + model_setting)
    print(str(confusion_matrix[0]) + "\n" + str(confusion_matrix[1]) + "\n")


def train_models(data_train, data_train_balanced, model_types, model_settings):
    for model_type in model_types:
        if model_type == "lr":
            lr_model_standard = LogisticRegression(labelCol="target", featuresCol="features")
            if "standard" in model_settings:
                yield lr_model_standard.fit(data_train), "lr", "standard"
            if "weighted" in model_settings:
                lr_model_weighted = LogisticRegression(labelCol="target", featuresCol="features", weightCol="weight")
                yield lr_model_weighted.fit(data_train), "lr", "weighted"
            if "balanced" in model_settings:
                yield lr_model_standard.fit(data_train_balanced), "lr", "balanced"
        elif model_type == "svm":
            svm_model_standard = LinearSVC(labelCol="target", featuresCol="features")
            if "standard" in model_settings:
                yield svm_model_standard.fit(data_train), "svm", "standard"
            if "weighted" in model_settings:
                svm_model_weighted = LinearSVC(labelCol="target", featuresCol="features", weightCol="weight")
                yield svm_model_weighted.fit(data_train), "svm", "weighted"
            if "balanced" in model_settings:
                yield svm_model_standard.fit(data_train_balanced), "svm", "balanced"
        elif model_type == "nb":
            nb_model_standard = NaiveBayes(labelCol="target", featuresCol="features")
            if "standard" in model_settings:
                yield nb_model_standard.fit(data_train), "nb", "standard"
            if "weighted" in model_settings:
                nb_model_weighted = NaiveBayes(labelCol="target", featuresCol="features", weightCol="weight")
                yield nb_model_weighted.fit(data_train), "nb", "weighted"
            if "balanced" in model_settings:
                yield nb_model_standard.fit(data_train_balanced), "nb", "balanced"
        elif model_type == "dt":
            dt_model = DecisionTreeClassifier(labelCol="target", featuresCol="features")
            if "standard" in model_settings:
                yield dt_model.fit(data_train), "dt", "standard"
            if "balanced" in model_settings:
                yield dt_model.fit(data_train_balanced), "dt", "balanced"
        else:  # model_type == "rf"
            rf_model = RandomForestClassifier(labelCol="target", featuresCol="features")
            if "standard" in model_settings:
                yield rf_model.fit(data_train), "rf", "standard"
            if "balanced" in model_settings:
                yield rf_model.fit(data_train_balanced), "rf", "balanced"


_model_types = ("lr", "svm", "nb", "dt", "rf")
_model_settings = ("standard", "weighted", "balanced")
_data_train, _data_test, _data_train_balanced = setup()
_trained_models = train_models(_data_train, _data_train_balanced, _model_types, _model_settings)
for _trained_model, _model_type, _model_setting in _trained_models:
    evaluate(_trained_model, _data_test, _model_type, _model_setting)
