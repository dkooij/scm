"""
Read pages from HDFS as Resilient Distributed Dataset.
Author: Daan Kooij
Last modified: September 28th, 2021
"""

from pyspark import SparkContext, Row
from pyspark.sql import SparkSession

from data_point import DataPoint
import detect_html
import extractor


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SPARKTEST-S1839047")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


BinaryTriple = Row("Stage file", "URL index", "Binary data")


def get_html_features(binary):
    page_html = detect_html.get_html(binary)
    data_point = DataPoint()
    extractor.set_html_features(page_html, data_point)
    return [v for k, v in sorted(data_point.features.items())]


def process_binary_tuple(binary_tuple):
    # Converts (file_path, binary_data)-tuple to
    # (stage_file, url_index, binary_data)-triple.
    (file_path, binary_data) = binary_tuple
    file_name = file_path.split("/")[-1]
    [stage_file, url_index] = file_name.split("-")
    return BinaryTriple(stage_file, url_index, bytearray(binary_data))


def crawl_to_rdd(crawl_directory):
    df_log_entries = spark.read.csv(crawl_directory + "/*.csv", header=True)
    df_binary_files = sc.binaryFiles(crawl_directory + "/pages").map(process_binary_tuple).toDF()

    combined_df = df_log_entries.join(df_binary_files, ["Stage file", "URL index"])
    print(combined_df.count())
    print(combined_df.take(2))


crawl_to_rdd("/user/s1839047/sparktest/testday")


# rdd2 = rdd_binary_files.map(lambda x: ([x[0]] + get_html_features(x[1])))
# print(rdd2.take(5))
