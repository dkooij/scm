"""
Calculate accuracies of datapoint sample plotted over RF/LR heatmaps.
Author: Daan Kooij
Last modified: March 10th, 2022
"""

from pyspark import SparkContext
from pyspark.ml.classification import LogisticRegressionModel, RandomForestClassificationModel
from pyspark.ml.linalg import DenseVector
from pyspark.sql import Row, SparkSession


DATASET_PATH = "extracted/graph-heatmap-sample-dataset.csv"


sc = SparkContext(appName="SCM-ML-HEATMAP-SAMPLE-PERFORMANCE")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def load_sample_dataset(target_features):
    df = spark.read.csv(DATASET_PATH)
    df = df.rdd.map(lambda row: Row(**{"features": DenseVector([int(row[x]) for x in target_features]),
                                       "label": int(row[9])})).toDF()
    return df


def load_models(model_type, model_name, feature_pair):
    feature1, feature2 = feature_pair
    full_model_name = model_name + "-" + str(min(feature1, feature2)) + "-" + str(max(feature1, feature2))
    model_path = "models/projections/" + full_model_name + ".model"
    if model_type == "rf":
        return RandomForestClassificationModel.load(model_path)
    elif model_type == "lr":
        return LogisticRegressionModel.load(model_path)


def evaluate(model_name, feature_pair):
    model = load_models(model_name, model_name, feature_pair)
    dataset = load_sample_dataset(feature_pair)
    pred = model.transform(dataset)
    return pred.rdd.map(lambda r: r["label"] == r["prediction"]).filter(lambda b: b).count() / pred.count()


for _model_name in ["rf", "lr"]:
    for _feature_pair in [(8, 5), (8, 3), (8, 6), (5, 3), (5, 6), (3, 6)]:
        print(_model_name, _feature_pair, evaluate(_model_name, _feature_pair))
