"""
Read and process pages from the HDFS as a Resilient Distributed Dataset.
Author: Daan Kooij
Last modified: November 30th, 2021
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession
import zlib

import detect_html
from get_page_links import get_page_links
from get_page_text import get_page_text
import global_vars


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


def extract_links(entry):
    page_html = detect_html.get_html(entry["Binary data"])
    if page_html:
        internal_outlinks, external_outlinks = get_page_links(entry["URL"], page_html)
        page_html.decompose()
    else:
        internal_outlinks, external_outlinks = [], []
    entry["Internal outlinks"] = internal_outlinks
    entry["External outlinks"] = external_outlinks
    return entry


def purge_binary_data(entry):
    del entry["Binary data"]
    return entry


def get_raw_rdd(crawl_directory, day_dir):
    return sc.pickleFile(crawl_directory + "/" + day_dir).mapValues(decompress_data)


def extract_features(rdd):
    rdd = rdd.mapValues(extract_text)
    rdd = rdd.mapValues(extract_links)
    rdd = rdd.mapValues(purge_binary_data)
    return rdd


def get_day_pairs(raw_rdds):
    pair_rdds = []
    for day1_rdd, day2_rdd in zip(raw_rdds, raw_rdds[1:]):
        pair_rdd = day1_rdd.join(day2_rdd)
        pair_rdds.append(pair_rdd)
    return pair_rdds


def compute_has_changed(pair_rdds):
    def map_tuple(entry_pair):
        entry1, entry2 = entry_pair
        day1, day2 = entry1["Day"], entry2["Day"]
        page_text1, page_text2 = entry1["Page text"], entry2["Page text"]
        page_ilinks1, page_ilinks2 = entry1["Internal outlinks"], entry2["Internal outlinks"]
        page_elinks1, page_elinks2 = entry1["External outlinks"], entry2["External outlinks"]
        change_bools = (page_text1 != page_text2,
                        page_ilinks1 != page_ilinks2,
                        page_elinks1 != page_elinks2)
        return day1, day2, change_bools

    change_rdds = []
    for pair_rdd in pair_rdds:
        change_rdds.append(pair_rdd.mapValues(map_tuple))
    return change_rdds


def save_change_rdd_as_csv(change_rdd, output_directory, output_name):
    output_path = output_directory + "/change-" + output_name

    def to_csv_line(rdd_entry):
        file_name, (day1, day2, change_bools) = rdd_entry
        change_str = ",".join("1" if b else "0" for b in change_bools)
        return ",".join((file_name, day1, day2, change_str))

    change_rdd.map(to_csv_line).saveAsTextFile(output_path)


def process(crawl_dir, extract_dir, days):
    day1_rdd, day2_rdd = None, None
    for day_dir in days:
        day1_rdd = day2_rdd
        day2_rdd = extract_features(get_raw_rdd(crawl_dir, day_dir + ".pickle"))
        if day1_rdd is not None:
            feature_rdd_list = [day1_rdd, day2_rdd]
            pair_rdd_list = get_day_pairs(feature_rdd_list)
            change_rdd_list = compute_has_changed(pair_rdd_list)
            save_change_rdd_as_csv(change_rdd_list[0], extract_dir, day_dir)


_crawl_dir = "/user/s1839047/crawls/data"
_extract_dir = "/user/s1839047/extracted"
_days = global_vars.DAYS
process(_crawl_dir, _extract_dir, _days)
