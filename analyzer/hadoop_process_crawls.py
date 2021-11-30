"""
Read and process pages from the HDFS as a Resilient Distributed Dataset.
Author: Daan Kooij
Last modified: November 30th, 2021
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys
import zlib

from get_page_text import get_page_text


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SCM-PROCESS-CRAWLS-S1839047")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def decompress_data(entry):
    entry["Binary data"] = zlib.decompress(entry["Binary data compressed"])
    del entry["Binary data compressed"]
    return entry


def extract_text(entry):
    entry["Page text"] = get_page_text(entry["Binary data"], one_line=False, wrap_text=False)
    return entry


def purge_binary_data(entry):
    del entry["Binary data"]
    return entry


def get_raw_rdd(crawl_directory, day_dir):
    return sc.pickleFile(crawl_directory + "/" + day_dir).mapValues(decompress_data)


def compute_raw_rdds(crawl_root, days):
    return [get_raw_rdd(crawl_root, day) for day in days]


def extract_features(rdd):
    rdd = rdd.mapValues(extract_text)
    rdd = rdd.mapValues(purge_binary_data)
    return rdd


def get_day_pairs(raw_rdds):
    pair_rdds = []
    for day1_rdd, day2_rdd in zip(raw_rdds, raw_rdds[1:]):
        pair_rdd = day1_rdd.join(day2_rdd)
        pair_rdds.append(pair_rdd)
    return pair_rdds


def compute_text_has_changed(pair_rdds):
    def map_tuple(entry_pair):
        entry1, entry2 = entry_pair
        day1, day2 = entry1["Day"], entry2["Day"]
        page_text1, page_text2 = entry1["Page text"], entry2["Page text"]
        has_changed = page_text1 != page_text2
        return day1, day2, has_changed

    change_rdds = []
    for pair_rdd in pair_rdds:
        change_rdds.append(pair_rdd.mapValues(map_tuple))
    return change_rdds


def combine_rdds(rdds):
    union_rdd = rdds[0]
    for next_rdd in rdds[1:]:
        union_rdd = union_rdd.union(next_rdd)
    return union_rdd


def save_change_rdd_as_csv(change_rdd, output_directory, output_name):
    output_path = output_directory + "/change-" + output_name

    def to_csv_line(rdd_entry):
        file_name, (day1, day2, has_changed) = rdd_entry
        return ",".join([file_name, day1, day2, str(has_changed)])

    change_rdd.map(to_csv_line).saveAsTextFile(output_path)


# crawl_dir = "/user/s1839047/crawls_test"
# day_list = ["miniday", "miniday2"]
# extract_dir = "/user/s1839047/extracted_test"


crawl_dir = "/user/s1839047/crawls/data"
extract_dir = "/user/s1839047/extracted"


def process(day1_dir, day2_dir):
    raw_rdd_list = compute_raw_rdds(crawl_dir, [day1_dir, day2_dir])
    feature_rdd_list = [extract_features(rdd) for rdd in raw_rdd_list]
    pair_rdd_list = get_day_pairs(feature_rdd_list)
    change_rdd_list = compute_text_has_changed(pair_rdd_list)
    save_change_rdd_as_csv(change_rdd_list[0], extract_dir, day2_dir)


if len(sys.argv) >= 3:
    dir1, dir2 = sys.argv[1], sys.argv[2]
    process(dir1, dir2)
else:
    print("Invalid usage")
