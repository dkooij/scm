"""
Create 1% samples of big crawls.
Author: Daan Kooij
Last modified: November 30th, 2021
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession


# Initialize Spark and SparkSQL context.
sc = SparkContext(appName="SCM-SAMPLE-CRAWLS-S1839047")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


def filter_criteria(key_value_pair):
    return int(key_value_pair[0].split("-")[-1]) % 100 == 42


def get_rdd_sample(crawl_directory, day_dir):
    return sc.pickleFile(crawl_directory + "/" + day_dir).filter(filter_criteria)


def save_rdd_as_pickle(output_root, day_dir, rdd):
    output_directory = output_root + "/" + day_dir
    rdd.saveAsPickleFile(output_directory)


days = ["20210612", "20210613", "20210614", "20210615", "20210616", "20210617", "20210618", "20210619", "20210620",
        "20210621", "20210622", "20210623", "20210624", "20210625", "20210626", "20210627", "20210628", "20210629",
        "20210630", "20210701", "20210702", "20210703", "20210704", "20210705", "20210706", "20210707", "20210708",
        "20210709", "20210710", "20210711", "20210712", "20210713", "20210714", "20210715", "20210716", "20210717",
        "20210718", "20210719", "20210720", "20210721", "20210722", "20210723", "20210724", "20210725", "20210726",
        "20210727", "20210728", "20210729", "20210730", "20210731", "20210801", "20210802", "20210803", "20210804",
        "20210805", "20210806", "20210807", "20210808", "20210809", "20210810", "20210811", "20210812", "20210813",
        "20210814", "20210815", "20210816", "20210817", "20210818", "20210819", "20210820", "20210821", "20210822",
        "20210823", "20210824", "20210825", "20210826", "20210827", "20210828", "20210829", "20210830", "20210831",
        "20210901", "20210902", "20210903", "20210904", "20210905", "20210906", "20210907", "20210908", "20210909"]
for day in days:
    rdd = get_rdd_sample("/user/s1839047/crawls/data", day + ".pickle").coalesce(30)
    save_rdd_as_pickle("/user/s1839047/crawls_sample", day + ".pickle", rdd)
