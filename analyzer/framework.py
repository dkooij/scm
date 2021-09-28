"""
Data pre-processing framework.
Takes a collection of web crawls, and converts it to pre-processed data points.
Author: Daan Kooij
Last modified: September 28th, 2021
"""

import csv
from datetime import datetime
import itertools
from multiprocessing import Process
import os

import csv_reader
import detect_html
import extractor
import train_test


CRAWLS_ROOT = "T:/crawls"
EXTRACT_ROOT = "T:/extracted"
FIRST_STAGE_PATH = "T:/stages/links_s00.txt"
PAGE_TEXT_DIR = "text"
STATIC_FEATURES_DIR = "static-features"
SV_DIR = "semantic"
TRAIN_TEST_DIR = "train-test"

TRAIN_FRACTION = 0.8


def get_timestamp():
    return datetime.now().strftime("%H:%M:%S") + " - "


def _extract_page_features_text(log_path, crawl_dir, text_output_filepath, features_output_filepath):
    # Extracts both static simple page features and page text
    with open(text_output_filepath, "w", newline="", encoding="utf-8") as text_output_file:
        text_output_writer = csv.writer(text_output_file)
        text_output_writer.writerow(["Stage file", "URL index", "URL", "Page text"])

        with open(features_output_filepath, "w", newline="", encoding="utf-8") as features_output_file:
            features_output_writer = csv.writer(features_output_file)
            features_header = []

            for log_entry in csv_reader.get_log_entries(log_path):
                with open(csv_reader.get_filepath(log_entry, crawl_dir), "rb") as file:
                    page_html = detect_html.get_html(file)
                    if page_html:
                        # Retrieve and write page text to CSV
                        page_text, page_words = detect_html.get_page_text(page_html)
                        text_output_writer.writerow([log_entry["Stage file"], log_entry["URL index"],
                                                     log_entry["URL"], page_text])

                        # Retrieve and write static page features to CSV
                        data_point = extractor.extract_static_features(log_entry, page_html,
                                                                       input_dir=crawl_dir,
                                                                       page_words=page_words)
                        if len(features_header) == 0:
                            features_header = ["Stage file", "URL index", "URL"] + \
                                              sorted(list(data_point.features.keys()))
                            features_output_writer.writerow(features_header)
                        feature_values = [v for k, v in sorted(list(data_point.features.items()))]
                        features_output_writer.writerow([log_entry["Stage file"], log_entry["URL index"],
                                                         log_entry["URL"]] + feature_values)


def extract_page_features_text(target, batch_index=0):
    processes = []

    text_output_dir = EXTRACT_ROOT + "/" + target + "/" + PAGE_TEXT_DIR
    os.makedirs(text_output_dir, exist_ok=True)
    features_output_dir = EXTRACT_ROOT + "/" + target + "/" + STATIC_FEATURES_DIR
    os.makedirs(features_output_dir, exist_ok=True)

    crawl_dir = CRAWLS_ROOT + "/" + target
    for tid, log_path in zip(itertools.count(), csv_reader.get_log_paths(crawl_dir)):
        text_output_filepath = text_output_dir + "/" + "text-" + str(tid) + ".csv"
        features_output_filepath = features_output_dir + "/" + "static-features-" + str(tid) + ".csv"
        process = Process(target=_extract_page_features_text, args=(
            log_path, crawl_dir, text_output_filepath, features_output_filepath))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    print(get_timestamp() + "Finished static feature and text extraction batch " + str(batch_index))


def extract_semantic_vectors(target, batch_index=0, start_index=0):
    import embedding  # Local import, because computationally expensive

    model_quad = embedding.get_model_quad()

    text_dir = EXTRACT_ROOT + "/" + target + "/" + PAGE_TEXT_DIR
    tensor_dir = EXTRACT_ROOT + "/" + target + "/" + SV_DIR
    os.makedirs(tensor_dir, exist_ok=True)

    for i, log_path in zip(itertools.count(start=batch_index), csv_reader.get_log_paths(text_dir)):
        if i >= start_index:
            for log_entry in csv_reader.get_log_entries(log_path, ignore_validity_check=True):
                embedding.compute_embedding(log_entry, model_quad, tensor_dir)
            print(get_timestamp() + "Finished semantic embedding batch " + str(i))


def run():
    target_list = ["20210612", "20210613", "20210614", "20210615", "20210616", "20210617", "20210618",
                   "20210619", "20210620", "20210621", "20210622", "20210623", "20210624", "20210625",
                   "20210626", "20210627", "20210628", "20210629", "20210630", "20210701", "20210702"]

    # Starting point configuration parameters
    should_split_train_test = True
    start_index_text = 0
    start_index_embedding = 0

    # Split domains into training and testing domains
    if should_split_train_test:
        train_test.split_domains(FIRST_STAGE_PATH, EXTRACT_ROOT, TRAIN_TEST_DIR, TRAIN_FRACTION)

    # Iterate over the crawls of all days in target_list
    for i, target in zip(itertools.count(), target_list):
        print(get_timestamp() + "Start processing daily crawl " + target)
        if i >= start_index_text:
            extract_page_features_text(target, batch_index=i)
        if i * 8 >= start_index_embedding - 7:
            extract_semantic_vectors(target, batch_index=i*8, start_index=start_index_embedding)
        print(get_timestamp() + "Finished processing daily crawl " + target + "\n")


if __name__ == "__main__":
    run()
