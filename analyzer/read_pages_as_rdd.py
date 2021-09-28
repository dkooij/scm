"""
Read pages from HDFS as Resilient Distributed Dataset.
Author: Daan Kooij
Last modified: September 28th, 2021
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession

from data_point import DataPoint
import detect_html
import extractor


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SPARKTEST-S1839047")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def get_html_features(binary):
    page_html = detect_html.get_html(binary)
    data_point = DataPoint()
    extractor.set_html_features(page_html, data_point)
    return [v for k, v in sorted(data_point.features.items())]


rdd = sc.binaryFiles("/user/s1839047/sparktest/testday/pages")
rdd2 = rdd.map(lambda x: ([x[0]] + get_html_features(x[1])))
print(rdd2.take(5))
