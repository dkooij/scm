"""
Module used to demonstrate how to read compressed feature RDDs.
Author: Daan Kooij
Last modified: December 16th, 2021
"""

import pickle
from pyspark import SparkContext
from pyspark.sql import SparkSession
import zlib


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SCM-DECOMPRESS-FEATURE-RDD-EXAMPLE")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def decompress_values(rdd):
    def decompress(entry):
        return pickle.loads(zlib.decompress(entry))
    return rdd.mapValues(decompress)


rdd = sc.pickleFile("/user/s1839047/extracted/features/features-20210614.pickle")
for (k, v), (k2, v2) in zip(rdd.take(1), decompress_values(rdd).take(1)):
    print(k, v, "\n", k2, v2)
print(rdd.count())  # Print the number of data entries
