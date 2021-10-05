"""
Read pages from HDFS as Resilient Distributed Dataset.
Author: Daan Kooij
Last modified: October 5th, 2021
"""

from collections import defaultdict
from pyspark import SparkContext
from pyspark.sql import SparkSession

import detect_html
import extractor


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SPARKTEST-S1839047")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def get_log_entry_rdd(crawl_directory):
    log_entry_df = spark.read.csv(crawl_directory + "/*.csv", header=True)
    log_entry_rdd = log_entry_df.rdd

    def convert(log_entry_row):
        # Converts Row-object to (file_name, log_entry_dict)-tuple.
        log_entry_dict = log_entry_row.asDict()
        file_name = log_entry_dict["Stage file"] + "-" + log_entry_dict["URL index"]
        del log_entry_dict["Stage file"]
        del log_entry_dict["URL index"]
        return file_name, log_entry_dict

    return log_entry_rdd.map(convert)


def get_binary_file_rdd(crawl_directory):
    binary_file_rdd = sc.binaryFiles(crawl_directory + "/pages")

    def convert(binary_tuple):
        # Converts (file_path, binary_data)-tuple to (file_name, binary_data)-tuple.
        (file_path, binary_data) = binary_tuple
        file_name = file_path.split("/")[-1]
        return file_name, binary_data

    return binary_file_rdd.map(convert)


def combine_log_entry_binary_file_rdds(log_entry_rdd, binary_file_rdd):
    joined_rdd = log_entry_rdd.join(binary_file_rdd)

    def fix_joined_tuple(joined_tuple):
        # Transforms (file_name, (log_entry, binary_data))-tuple to (file_name, log_entry)-tuple,
        # where log_entry has an additional dictionary entry for binary_data.
        (file_name, (log_entry, binary_data)) = joined_tuple
        log_entry["Binary data"] = binary_data
        return file_name, log_entry

    return joined_rdd.map(fix_joined_tuple)


def extract_data_points(raw_rdd):
    def extract_data_point(log_entry_tuple):
        # Converts (file_name, log_entry)-tuple to (file_name, data_point)-tuple.
        file_name, log_entry = log_entry_tuple
        page_html = detect_html.get_html(log_entry["Binary data"])
        if page_html:
            data_point = extractor.extract_static_features(log_entry, page_html)
        else:
            data_point = None
        return file_name, data_point

    def is_valid_page(data_point_tuple):
        _, data_point = data_point_tuple
        return data_point is not None

    return raw_rdd.map(extract_data_point).filter(is_valid_page)


def crawl_to_raw_rdd(crawl_root, day_dir, extract_dir):
    crawl_directory = crawl_root + "/" + day_dir
    raw_rdd_path = extract_dir + "/raw_rdds/" + day_dir + ".pickle"

    log_entry_rdd = get_log_entry_rdd(crawl_directory)
    binary_file_rdd = get_binary_file_rdd(crawl_directory)
    raw_rdd = combine_log_entry_binary_file_rdds(log_entry_rdd, binary_file_rdd)
    raw_rdd.saveAsPickleFile(raw_rdd_path)


def raw_rdd_to_data_points(day_dir, extract_dir):
    raw_rdd_path = extract_dir + "/raw_rdds/" + day_dir + ".pickle"
    data_point_path = extract_dir + "/data_points/" + day_dir + ".pickle"

    raw_rdd = sc.pickleFile(raw_rdd_path)
    data_point_rdd = extract_data_points(raw_rdd)
    data_point_rdd.saveAsPickleFile(data_point_path)


def compute_raw_rdds(crawl_root, days, extract_dir):
    for day in days:
        crawl_to_raw_rdd(crawl_root, day, extract_dir)


def get_day_pairs(days, extract_dir):
    day1_rdd, day2_rdd = None, None

    for day1, day2 in zip(days, days[1:]):
        rdd1_path = extract_dir + "/raw_rdds/" + day1 + ".pickle"
        rdd2_path = extract_dir + "/raw_rdds/" + day2 + ".pickle"
        day1_rdd = sc.pickleFile(rdd1_path) if day2_rdd is None else day2_rdd
        day2_rdd = sc.pickleFile(rdd2_path)

        pair_path = extract_dir + "/raw_pairs/" + day1[:8] + "-" + day2[:8] + ".pickle"
        pair_rdd = day1_rdd.join(day2_rdd)
        pair_rdd.saveAsPickleFile(pair_path)


def combine_raw_pair_rdds(days, extract_dir):
    # TODO: untested
    first_day, last_day, union_rdd = None, None, None
    for day1, day2 in zip(days, days[1:]):
        pair_path = extract_dir + "/raw_pairs/" + day1[:8] + "-" + day2[:8] + ".pickle"
        new_pair_rdd = sc.pickleFile(pair_path)
        # TODO: add day-pair (day1,day2) field to RDD entries

        if union_rdd is None:
            union_rdd = new_pair_rdd
            first_day = day1[:8]
        else:
            union_rdd = union_rdd.union(new_pair_rdd)
        last_day = day2[:8]

    union_path = extract_dir + "/raw_unions/" + first_day + "-" + last_day + ".pickle"
    union_rdd.saveAsPickleFile(union_path)


day_list = ["20210612000004", "20210613000001"]
# compute_raw_rdds("/user/s1839047/crawls", day_list, "/user/s1839047/extracted")
get_day_pairs(day_list, "/user/s1839047/extracted")
