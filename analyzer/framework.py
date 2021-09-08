"""
Data pre-processing framework.
Takes a collection of web crawls, and converts it to pre-processed data points.
Author: Daan Kooij
Last modified: September 8th, 2021
"""


import csv
import itertools
from multiprocessing import Process
import os

import csv_reader
import detect_html
from embedding import get_page_text


BATCH_SIZE = 1000
CRAWLS_ROOT = "D:/crawls"
EXTRACT_ROOT = "D:/extracted"
PAGE_TEXT_DIR = "text"



target_list = ["20210612"]


def extract_page_text(log_path, crawl_dir, output_filepath):
    with open(output_filepath, "w", newline="") as output_file:
        output_writer = csv.writer(output_file)
        output_writer.writerow(["Stage file", "URL index", "URL", "Page text"])

        for log_entry in csv_reader.get_log_entries(log_path):
            with open(csv_reader.get_filepath(log_entry, crawl_dir), "rb") as file:
                page_html = detect_html.get_html(file)
                page_text = get_page_text(page_html)
                output_writer.writerow([log_entry["Stage file"], log_entry["URL index"], log_entry["URL"], page_text])

    print(log_path + " done")


def fun():
    for target in target_list:
        output_dir = EXTRACT_ROOT + "/" + target + "/" + PAGE_TEXT_DIR
        os.makedirs(output_dir, exist_ok=True)

        crawl_dir = CRAWLS_ROOT + "/" + target
        for tid, log_path in zip(itertools.count(), csv_reader.get_log_paths(crawl_dir)):
            output_filepath = output_dir + "/" + "text-" + str(tid) + ".csv"
            # extract_page_text(log_path, crawl_dir, output_filepath)
            Process(target=extract_page_text, args=(log_path, crawl_dir, output_filepath)).start()


if __name__ == "__main__":
    fun()
