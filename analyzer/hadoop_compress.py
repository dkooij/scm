"""
Read pages and log files from the HDFS, compress the data, and save it back to the HDFS.
Author: Daan Kooij
Last modified: November 23rd, 2021
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as SparkFunction
import sys
import zlib


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SCM-COMPRESS-S1839047")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def get_log_entry_rdd(crawl_directory, day_dir):
    log_entry_df = spark.read.csv(crawl_directory + "/*.csv", header=True)
    log_entry_df = log_entry_df.withColumn("Log file", SparkFunction.input_file_name())  # Add log file column
    log_entry_rdd = log_entry_df.rdd
    day = day_dir[:8]

    def convert(log_entry_row):
        # Converts Row-object to (file_name, log_entry_dict)-tuple.
        log_entry_dict = log_entry_row.asDict()
        file_name = log_entry_dict["Stage file"] + "-" + log_entry_dict["URL index"]

        log_entry_dict["Day"] = day
        log_entry_dict["Log index"] = int(log_entry_dict["Log file"].split("-")[-1].split(".")[0])
        log_entry_dict["Stage index"] = int(log_entry_dict["Stage file"].split("_s")[-1])
        log_entry_dict["URL index"] = int(log_entry_dict["URL index"])

        del log_entry_dict["Log file"]
        del log_entry_dict["Stage file"]

        return file_name, log_entry_dict

    return log_entry_rdd.map(convert)


def get_binary_file_rdd(crawl_directory):
    binary_file_rdd = sc.binaryFiles(crawl_directory + "/pages")

    def convert(binary_tuple):
        # Converts (file_path, binary_data)-tuple to (file_name, binary_data)-tuple.
        (file_path, binary_data) = binary_tuple
        file_name = file_path.split("/")[-1]
        binary_data_compressed = zlib.compress(binary_data)
        return file_name, binary_data_compressed

    return binary_file_rdd.map(convert)


def combine_log_entry_binary_file_rdds(log_entry_rdd, binary_file_rdd):
    joined_rdd = log_entry_rdd.join(binary_file_rdd)

    def fix_joined_tuple(joined_tuple):
        # Transforms (file_name, (log_entry, binary_data))-tuple to (file_name, log_entry)-tuple,
        # where log_entry has an additional dictionary entry for binary_data.
        (file_name, (log_entry, binary_data_compressed)) = joined_tuple
        log_entry["Binary data compressed"] = binary_data_compressed
        return file_name, log_entry

    return joined_rdd.map(fix_joined_tuple)


def crawl_to_raw_rdd(crawl_root, day_dir):
    crawl_directory = crawl_root + "/" + day_dir

    log_entry_rdd = get_log_entry_rdd(crawl_directory, day_dir)
    binary_file_rdd = get_binary_file_rdd(crawl_directory)
    joined_rdd = combine_log_entry_binary_file_rdds(log_entry_rdd, binary_file_rdd)
    return joined_rdd


def save_rdd_as_pickle(output_root, day_dir, rdd):
    output_directory = output_root + "/" + day_dir
    rdd.saveAsPickleFile(output_directory)


def load_compress_store_crawl_data(day_dir):
    joined_rdd = crawl_to_raw_rdd(crawl_dir, day_dir)
    save_rdd_as_pickle(output_dir, day_dir, joined_rdd)


crawl_dir = "/user/s1839047/crawls_test"
output_dir = "/user/s1839047/crawls_test_compressed"


if len(sys.argv) >= 2:
    _dir = sys.argv[1]
    load_compress_store_crawl_data(_dir)
else:
    print("Invalid usage")
