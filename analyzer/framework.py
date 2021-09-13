"""
Data pre-processing framework.
Takes a collection of web crawls, and converts it to pre-processed data points.
Author: Daan Kooij
Last modified: September 13th, 2021
"""

import csv
import itertools
from multiprocessing import Process
import os

import csv_reader
import detect_html


BATCH_SIZE = 1000
CRAWLS_ROOT = "D:/crawls"
EXTRACT_ROOT = "D:/extracted"
PAGE_TEXT_DIR = "text"
SV_DIR = "semantic"


def _extract_page_text(log_path, crawl_dir, output_filepath):
    with open(output_filepath, "w", newline="", encoding="utf-8") as output_file:
        output_writer = csv.writer(output_file)
        output_writer.writerow(["Stage file", "URL index", "URL", "Page text"])

        for log_entry in csv_reader.get_log_entries(log_path):
            with open(csv_reader.get_filepath(log_entry, crawl_dir), "rb") as file:
                page_html = detect_html.get_html(file)
                if page_html:
                    page_text = detect_html.get_page_text(page_html)
                    output_writer.writerow([log_entry["Stage file"], log_entry["URL index"],
                                            log_entry["URL"], page_text])

    print(log_path + " done")


def extract_page_text(target):
    processes = []
    output_dir = EXTRACT_ROOT + "/" + target + "/" + PAGE_TEXT_DIR
    os.makedirs(output_dir, exist_ok=True)

    crawl_dir = CRAWLS_ROOT + "/" + target
    for tid, log_path in zip(itertools.count(), csv_reader.get_log_paths(crawl_dir)):
        output_filepath = output_dir + "/" + "text-" + str(tid) + ".csv"
        process = Process(target=_extract_page_text, args=(log_path, crawl_dir, output_filepath))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()


def extract_semantic_vectors(target):
    # Local import, because computationally expensive
    import embedding

    model_quad = embedding.get_model_quad()

    text_dir = EXTRACT_ROOT + "/" + target + "/" + PAGE_TEXT_DIR
    tensor_dir = EXTRACT_ROOT + "/" + target + "/" + SV_DIR
    os.makedirs(tensor_dir, exist_ok=True)

    for log_entry in csv_reader.get_all_log_entries(text_dir, ignore_validity_check=True):
        embedding.compute_embedding(log_entry, model_quad, tensor_dir)


def run():
    target_list = ["20210612"]

    for target in target_list:
        extract_page_text(target)
        extract_semantic_vectors(target)


if __name__ == "__main__":
    run()
