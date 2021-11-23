"""
Module used to demonstrate how to read compressed Pickle crawls.
Author: Daan Kooij
Last modified: November 23rd, 2021
"""

import hashlib
from pyspark import SparkContext
from pyspark.sql import SparkSession
import zlib


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SCM-DECOMPRESS-S1839047")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def decompress(log_entry):
    log_entry["Binary data"] = zlib.decompress(log_entry["Binary data compressed"])
    del log_entry["Binary data compressed"]
    return log_entry


rdd = sc.pickleFile("/user/s1839047/crawls_compressed/20210613")
print(list(rdd.first()[1].keys()))
# print(rdd.mapValues(decompress).first())
for k, v in rdd.mapValues(decompress).take(10):
    print(k, hashlib.md5(str(v["Binary data"]).encode("utf-8")).hexdigest())
print(rdd.count())
