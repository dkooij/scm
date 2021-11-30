"""
Create 1% samples of big crawls.
Author: Daan Kooij
Last modified: November 30th, 2021
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession

import global_vars


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SCM-SAMPLE-CRAWLS-S1839047")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def filter_criteria(key_value_pair):
    return int(key_value_pair[0].split("-")[-1]) % 100 == 42


def get_rdd_sample(crawl_directory, day_dir):
    return sc.pickleFile(crawl_directory + "/" + day_dir).filter(filter_criteria)


def save_rdd_as_pickle(output_root, day_dir, rdd):
    output_directory = output_root + "/" + day_dir
    rdd.saveAsPickleFile(output_directory)


days = global_vars.DAYS
for day in days:
    rdd = get_rdd_sample("/user/s1839047/crawls/data", day + ".pickle").coalesce(30)
    save_rdd_as_pickle("/user/s1839047/crawls_sample", day + ".pickle", rdd)
