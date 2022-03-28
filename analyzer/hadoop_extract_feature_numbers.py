"""
Convert feature RDDs to CSVs of feature numbers.
Author: Daan Kooij
Last modified: March 28th, 2022
"""

import pickle
from pyspark import SparkContext
from pyspark.sql import SparkSession
import zlib

import global_vars


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SCM-EXTRACT-FEATURE_NUMBERS")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def decompress_values(rdd):
    def decompress(entry):
        return pickle.loads(zlib.decompress(entry))
    return rdd.mapValues(decompress)


def extract_numbers_single(features, feature_names):
    feature_numbers = []
    for feature_name in feature_names:
        feature_numbers.append(len(features[feature_name]))
    return feature_numbers


def save_training_pair_rdd_as_csv(rdd, output_directory, day_dir):
    output_path = output_directory + "/feature-numbers-" + day_dir

    def to_csv_line(rdd_row):
        file_name, features = rdd_row
        page_id = file_name.split("_")[1]
        return day_dir + "," + page_id + "," + ",".join(str(v) for v in features)

    rdd.map(to_csv_line).saveAsTextFile(output_path)


def extract_feature_numbers(features_dir, extract_dir, days, feature_names):
    for day_dir in days:
        rdd = decompress_values(sc.pickleFile(features_dir + "/features-" + day_dir + ".pickle")).\
            mapValues(lambda v: extract_numbers_single(v, feature_names)).coalesce(1)
        save_training_pair_rdd_as_csv(rdd, extract_dir, day_dir)


_features_dir = "/user/s1839047/extracted/features"
_extract_dir = "/user/s1839047/extracted/feature-numbers"
_days = global_vars.DAYS
_feature_names = ["Email links", "External outlinks", "Images", "Internal outlinks",
                  "Metas", "Page text", "Scripts", "Tables", "Tags"]
extract_feature_numbers(_features_dir, _extract_dir, _days, _feature_names)
