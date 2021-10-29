"""
Read and process pages from the HDFS as a Resilient Distributed Dataset.
Author: Daan Kooij
Last modified: October 28th, 2021
"""

from pyspark import SparkContext, Row
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as SparkFunction
import sys

from get_page_text import get_page_text


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SCM-PROCESS-CRAWLS-S1839047")
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
        return file_name, binary_data

    return binary_file_rdd.map(convert)


def extract_text(binary_file_rdd):
    def extract(binary_tuple):
        # Extracts page HTML and page text from binary data.
        (file_name, binary_data) = binary_tuple
        page_text = get_page_text(binary_data)
        return file_name, page_text

    return binary_file_rdd.map(extract)


def combine_log_entry_page_text_rdds(log_entry_rdd, page_text_rdd):
    joined_rdd = log_entry_rdd.join(page_text_rdd)

    def fix_joined_tuple(joined_tuple):
        # Transforms (file_name, (log_entry, binary_data))-tuple to (file_name, log_entry)-tuple,
        # where log_entry has an additional dictionary entry for binary_data.
        (file_name, (log_entry, page_text)) = joined_tuple
        log_entry["Page text"] = page_text
        return file_name, log_entry

    return joined_rdd.map(fix_joined_tuple)


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
    # Filters out both pages for which the HTML is invalid (i.e., cannot be parsed),
    # pages that contain no main content (that have no text),
    # and pages that have another status code than HEADLESS_SUCCESS.
    # Note: unsure whether disregarding pages with no text is the right thing to do.

    def filter_invalid(joined_tuple):
        (_, log_entry) = joined_tuple
        page_text = log_entry["Page text"]
        status_code = log_entry["Status code"]
        return len(page_text) > 0 and status_code == "RequestStatus.HEADLESS_SUCCESS"

    return joined_rdd.filter(filter_invalid)


def filter_out_false_duplicates(joined_rdd):
    df = joined_rdd.map(lambda t: Row(file_name=t[0], page_text=t[1]["Page text"],
                                      partition=1, order_index=t[1]["Order index"])).toDF()
    rdd = df.withColumn("prev_page_text", SparkFunction.lag(df["page_text"])
                        .over(Window.partitionBy("partition").orderBy("order_index"))).rdd

    def is_valid(row):
        # Verifies for a row (representing a crawled page) whether the page is valid.
        page_text, prev_page_text = row["page_text"], row["prev_page_text"]
        return page_text != prev_page_text

    valid_check_rdd = rdd.filter(is_valid).map(lambda row: (row["file_name"], None))
    filtered_joined_rdd = joined_rdd.join(valid_check_rdd).map(lambda t: (t[0], t[1][0]))

    return filtered_joined_rdd


def crawl_to_raw_rdd(crawl_root, day_dir):
    crawl_directory = crawl_root + "/" + day_dir

    log_entry_rdd = get_log_entry_rdd(crawl_directory, day_dir)
    binary_file_rdd = get_binary_file_rdd(crawl_directory)
    page_text_rdd = extract_text(binary_file_rdd)
    joined_rdd = combine_log_entry_page_text_rdds(log_entry_rdd, page_text_rdd)
    joined_rdd = preserve_crawl_order(joined_rdd)
    joined_rdd = filter_out_invalid_pages(joined_rdd)
    joined_rdd = filter_out_false_duplicates(joined_rdd)
    return joined_rdd


def compute_raw_rdds(crawl_root, days):
    return [crawl_to_raw_rdd(crawl_root, day) for day in days]


"""
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


def raw_rdd_to_data_points(day_dir, extract_dir):
    raw_rdd_path = extract_dir + "/raw_rdds/" + day_dir + ".pickle"
    data_point_path = extract_dir + "/data_points/" + day_dir + ".pickle"

    raw_rdd = sc.pickleFile(raw_rdd_path)
    data_point_rdd = extract_data_points(raw_rdd)
    data_point_rdd.saveAsPickleFile(data_point_path)
"""


def get_day_pairs(raw_rdds):
    pair_rdds = []
    for day1_rdd, day2_rdd in zip(raw_rdds, raw_rdds[1:]):
        pair_rdd = day1_rdd.join(day2_rdd)
        pair_rdds.append(pair_rdd)
    return pair_rdds


def compute_has_changed(pair_rdds):
    def map_tuple(tuple_pair):
        file_name, (log_entry1, log_entry2) = tuple_pair
        day1, day2 = log_entry1["Day"], log_entry2["Day"]
        page_text1, page_text2 = log_entry1["Page text"], log_entry2["Page text"]
        has_changed = page_text1 != page_text2
        return file_name, (day1, day2, has_changed)

    change_rdds = []
    for pair_rdd in pair_rdds:
        change_rdds.append(pair_rdd.map(map_tuple))
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


crawl_dir = "/user/s1839047/crawls"
extract_dir = "/user/s1839047/extracted"


def process(day1_dir, day2_dir):
    raw_rdd_list = compute_raw_rdds(crawl_dir, [day1_dir, day2_dir])
    pair_rdd_list = get_day_pairs(raw_rdd_list)
    change_rdd_list = compute_has_changed(pair_rdd_list)
    save_change_rdd_as_csv(change_rdd_list[0], extract_dir, day2_dir)


if len(sys.argv) >= 3:
    dir1, dir2 = sys.argv[1], sys.argv[2]
    process(dir1, dir2)
else:
    print("Invalid usage")