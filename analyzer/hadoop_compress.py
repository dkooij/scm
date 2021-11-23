"""
Read pages and log files from the HDFS, compress the data, and save it back to the HDFS.
Author: Daan Kooij
Last modified: November 23rd, 2021
"""

from pyspark import SparkContext, Row
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as SparkFunction
import sys
import zlib

from get_page_text import get_page_text
import text_overlap


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


def extract_text(joined_rdd):
    def extract(joined_tuple):
        # Extracts page text from binary data.
        (file_name, log_entry) = joined_tuple
        log_entry["Page text"] = get_page_text(zlib.decompress(log_entry["Binary data compressed"]), one_line=False)
        return file_name, log_entry

    return joined_rdd.map(extract)


def preserve_crawl_order(joined_rdd):
    # Adds an "Order index" field to RDD rows preserving the original crawl order:
    # first order on crawler thread (log index), then on stage index, and finally on URL index.
    # These three fields to be sorted on are located in the log_entry dicts of each row.
    # The main purpose of this function is to be able to filter out invalid false duplicate pages,
    # by comparing whether consecutively crawled pages by the same thread have equal content.

    def add_order_index(joined_tuple):
        # Converts (file_name, log_entry)-tuple to sortable integer, which is then added to log_entry.
        # Assumes no more than 999.999 URLs crawled per stage, and no more than 1.000 stages.
        (file_name, log_entry) = joined_tuple
        log_index, stage_index, url_index = log_entry["Log index"], log_entry["Stage index"], log_entry["URL index"]
        order_index = log_index * 1000000000 + stage_index * 1000000 + url_index
        log_entry["Order index"] = order_index
        return file_name, log_entry

    return joined_rdd.map(add_order_index)


def filter_out_invalid_pages(joined_rdd):
    # Filters out pages that have another status code than HEADLESS_SUCCESS.

    def filter_invalid(joined_tuple):
        (_, log_entry) = joined_tuple
        status_code = log_entry["Status code"]
        return status_code == "RequestStatus.HEADLESS_SUCCESS"

    return joined_rdd.filter(filter_invalid)


def filter_out_false_duplicates(joined_rdd, max_overlap=0.75):
    df = joined_rdd.map(lambda t: Row(file_name=t[0], page_text=t[1]["Page text"],
                                      partition=1, order_index=t[1]["Order index"])).toDF()
    rdd = df.withColumn("prev_page_text", SparkFunction.lag(df["page_text"])
                        .over(Window.partitionBy("partition").orderBy("order_index"))).rdd

    def is_valid(row):
        # Verifies for a row (representing a crawled page) whether the page is valid.
        page_text, prev_page_text = row["page_text"], row["prev_page_text"]
        if prev_page_text is None:
            return True
        else:
            return text_overlap.get_overlap_fraction(prev_page_text, page_text) <= max_overlap

    valid_check_rdd = rdd.filter(is_valid).map(lambda row: (row["file_name"], None))
    filtered_joined_rdd = joined_rdd.join(valid_check_rdd).map(lambda t: (t[0], t[1][0]))

    return filtered_joined_rdd


def clean_up_log_entries(joined_rdd):
    # Remove unused fields from RDD.

    def clean_up(joined_tuple):
        (file_name, log_entry) = joined_tuple
        del log_entry["File present"]  # Every file is present at this point
        del log_entry["Order index"]  # Only used to filter out false duplicates, which has already been done
        del log_entry["Page text"]  # Can be deduced from the binary data
        del log_entry["Status code"]  # Every status code is HEADLESS_SUCCESS at this point
        return file_name, log_entry

    return joined_rdd.map(clean_up)


def crawl_to_raw_rdd(crawl_root, day_dir):
    crawl_directory = crawl_root + "/" + day_dir

    log_entry_rdd = get_log_entry_rdd(crawl_directory, day_dir)
    binary_file_rdd = get_binary_file_rdd(crawl_directory)
    joined_rdd = combine_log_entry_binary_file_rdds(log_entry_rdd, binary_file_rdd)
    joined_rdd = extract_text(joined_rdd)
    joined_rdd = preserve_crawl_order(joined_rdd)
    joined_rdd = filter_out_invalid_pages(joined_rdd)
    joined_rdd = filter_out_false_duplicates(joined_rdd)
    joined_rdd = clean_up_log_entries(joined_rdd)

    return joined_rdd


def save_rdd_as_pickle(output_root, day_dir, rdd):
    output_directory = output_root + "/" + day_dir
    rdd.saveAsPickleFile(output_directory)


def load_compress_store_crawl_data(day_dir):
    joined_rdd = crawl_to_raw_rdd(crawl_dir, day_dir)
    save_rdd_as_pickle(output_dir, day_dir[:8], joined_rdd)


crawl_dir = "/user/s1839047/crawls"
output_dir = "/user/s1839047/crawls_compressed"


if len(sys.argv) >= 2:
    _dir = sys.argv[1]
    load_compress_store_crawl_data(_dir)
else:
    print("Invalid usage")
