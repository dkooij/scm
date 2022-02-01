"""
Visualize how predictions by ML models are made by making predictions for all values in a grid.
Author: Daan Kooij
Last modified: February 1st, 2022
"""

import numpy as np
from pyspark import SparkContext
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.linalg import DenseVector
from pyspark.sql import SparkSession


sc = SparkContext(appName="SCM-ML-HEATMAP")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def load_model(model_type, model_name):
    if model_type == "rf":
        return RandomForestClassificationModel.load("models/" + model_name + ".model")


def create_feature_grid(feature_statistics, feature1, feature2, resolution):
    values = [t[2] for t in feature_statistics]
    vectors = []

    for x in sorted(list(set(np.linspace(feature_statistics[feature1][0], feature_statistics[feature1][1],
                                         resolution, dtype=int)))):
        values[feature1] = x
        for y in sorted(list(set(np.linspace(feature_statistics[feature2][0], feature_statistics[feature2][1],
                                             resolution, dtype=int)))):
            values[feature2] = y
            vectors.append(DenseVector(values))

    return spark.createDataFrame(data=[[vector] for vector in vectors], schema=["features"])


def create_heatmap(model_name, model, feature_statistics, feature1, feature2, resolution):
    feature_grid = create_feature_grid(feature_statistics, feature1, feature2, resolution)
    predictions = model.transform(feature_grid)

    def to_csv_line(rdd_row):
        features, probability = rdd_row["features"], rdd_row["probability"][0]
        return ",".join([str(int(features[feature1])), str(int(features[feature2])), str(probability)])

    output_path = "predicted/grid/" + model_name + "-" + str(feature1) + "-" + str(feature2) + ".csv"
    predictions.rdd.coalesce(1).map(to_csv_line).saveAsTextFile(output_path)


def create_heatmaps(model_type, model_name, feature_statistics, feature_pairs, resolution):
    model = load_model(model_type, model_name)
    for feature1, feature2 in feature_pairs:
        create_heatmap(model_name, model, feature_statistics, feature1, feature2, resolution)


_model_type, _model_name = "rf", "rf"
_feature_statistics = [(0, 3, 0), (0, 76, 5), (0, 86, 6), (0, 339, 41), (0, 37, 9),
                       (2, 792, 120), (0, 65, 14), (0, 8, 0), (6, 2186, 368)]
_feature_pairs = [(x, y) for x in range(9) for y in range(9) if x != y]
_resolution = 100

create_heatmaps(_model_type, _model_name, _feature_statistics, _feature_pairs, _resolution)
