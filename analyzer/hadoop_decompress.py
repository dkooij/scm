"""
Module used to demonstrate how to read compressed Pickle crawls.
Author: Daan Kooij
Last modified: December 1st, 2021
"""

import hashlib
from pyspark import SparkContext
from pyspark.sql import SparkSession
import zlib


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SCM-DECOMPRESS-CRAWLS-EXAMPLE")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def decompress(log_entry):
    log_entry["Binary data"] = zlib.decompress(log_entry["Binary data compressed"])
    del log_entry["Binary data compressed"]
    return log_entry


rdd = sc.pickleFile("/user/s1839047/crawls/data/20210612.pickle")
print(list(rdd.first()[1].keys()))  # Print the keys of the data entries
for k, v in rdd.mapValues(decompress).take(10):  # Print MD5 hashes of 10 pages
    print(k, hashlib.md5(str(v["Binary data"]).encode("utf-8")).hexdigest())
print(rdd.count())  # Print the number of data entries
