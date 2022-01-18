"""
Convert feature RDD to CSVs of static feature training pairs.
Author: Daan Kooij
Last modified: January 18th, 2022
"""

import pickle
from pyspark import SparkContext
from pyspark.sql import SparkSession
import zlib

import global_vars


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SCM-EXTRACT-STATIC-TRAINING-PAIRS")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def decompress_values(rdd):
    def decompress(entry):
        return pickle.loads(zlib.decompress(entry))
    return rdd.mapValues(decompress)


def extract_static_training_pair(entry_pair, feature_names, target_names):
    entry1, entry2 = entry_pair

    features = []
    for feature_name in feature_names:
        features.append(len(entry1[feature_name]))

    targets = []
    for target_name in target_names:
        targets.append(entry1[target_name] != entry2[target_name])

    return features, targets


def save_training_pair_rdd_as_csv(rdd, output_directory, output_name):
    output_path = output_directory + "/static-training-pairs-" + output_name

    def to_csv_line(rdd_row):
        file_name, (features, targets) = rdd_row
        page_id = file_name.split("_")[1]
        return page_id + "," + ",".join(str(v) for v in features) + "," + ",".join(str(int(v)) for v in targets)

    rdd.map(to_csv_line).saveAsTextFile(output_path)


def extract_static_training_pairs(features_dir, extract_dir, days, feature_names, target_names):
    day1_rdd, day2_rdd = None, None
    for day_dir in days:
        day1_rdd = day2_rdd
        day2_rdd = decompress_values(sc.pickleFile(features_dir + "/features-" + day_dir + ".pickle"))
        if day1_rdd is not None:
            combined_rdd = day1_rdd.join(day2_rdd).mapValues(lambda ep:
                extract_static_training_pair(ep, feature_names, target_names)).coalesce(1)
            save_training_pair_rdd_as_csv(combined_rdd, extract_dir, day_dir)


_features_dir = "/user/s1839047/extracted/features"
_extract_dir = "/user/s1839047/extracted/static-training-pairs-2"
_days = global_vars.DAYS
_feature_names = ["Email links", "External outlinks", "Images", "Internal outlinks",
                  "Metas", "Page text", "Scripts", "Tables", "Tags"]
_target_names = ["Page text", "Internal outlinks", "External outlinks"]
extract_static_training_pairs(_features_dir, _extract_dir, _days, _feature_names, _target_names)
