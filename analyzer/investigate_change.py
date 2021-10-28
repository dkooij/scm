"""
For a collection of crawled pages spanning two days,
investigate the type of changes the pages undergo.
Author: Daan Kooij
Last modified: October 28th, 2021
"""

import csv
from datetime import datetime
import itertools
from multiprocessing import Process
import os

import csv_reader
import detect_html
import extractor
from get_page_text import get_page_text


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
                    page_text = get_page_text(file.read())
                    file.seek(0)  # To allow reading the file again
                    page_html = detect_html.get_html(file)
                    if len(page_text) > 0 and page_html:
                        # Write page text to CSV
                        text_output_writer.writerow([log_entry["Stage file"], log_entry["URL index"],
                                                     log_entry["URL"], page_text])

                        # Retrieve and write static page features to CSV
                        page_words = page_text.split()
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


def extract_page_features_text(crawls_root, input_dir, output_dir):
    processes = []

    text_output_dir = output_dir + "/" + input_dir + "/text"
    features_output_dir = output_dir + "/" + input_dir + "/features"
    os.makedirs(text_output_dir, exist_ok=True)
    os.makedirs(features_output_dir, exist_ok=True)

    crawl_dir = crawls_root + "/" + input_dir
    for tid, log_path in zip(itertools.count(), csv_reader.get_log_paths(crawl_dir)):
        text_output_filepath = text_output_dir + "/text-" + str(tid) + ".csv"
        features_output_filepath = features_output_dir + "/features-" + str(tid) + ".csv"
        process = Process(target=_extract_page_features_text, args=(
            log_path, crawl_dir, text_output_filepath, features_output_filepath))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()


"""
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
"""


def combine_csv_files(input_dir, output_dir):
    """
    Combine collections of feature and text .csv files into
    respectively one combined feature file and one combined text file.
    """
    feature_path = output_dir + "/" + input_dir + "/features"
    text_path = output_dir + "/" + input_dir + "/text"

    feature_entries = []
    text_entries = []
    for entry in csv_reader.get_all_log_entries(feature_path, ignore_validity_check=True):
        feature_entries.append(entry)
    for entry in csv_reader.get_all_log_entries(text_path, ignore_validity_check=True):
        text_entries.append(entry)
    feature_entries.sort(key=lambda e: (e["Stage file"], int(e["URL index"])))
    text_entries.sort(key=lambda e: (e["Stage file"], int(e["URL index"])))

    feature_output = output_dir + "/" + input_dir + "/features.csv"
    text_output = output_dir + "/" + input_dir + "/text.csv"
    common_fields = ["Stage file", "URL index", "URL"]
    csv_reader.write_csv_file(feature_output, feature_entries, common_fields)
    csv_reader.write_csv_file(text_output, text_entries, common_fields)


# def compute_text_change(input_dir, output_dir, target_fields):
#     feature_path1 = output_dir + "/" + input1_dir + "/features.csv"
#     feature_path2 = output_dir + "/" + input2_dir + "/features.csv"
#     text_path1 = output_dir + "/" + input1_dir + "/text.csv"
#     text_path2 = output_dir + "/" + input2_dir + "/text.csv"


def run():
    # crawls_root = "C:/Users/daank/Drawer/SCM archives/Full crawls"
    # target_list = ["20210612", "20210613"]
    crawls_root = "C:/Users/daank/Drawer/SCM archives/Crawl samples"
    target_list = ["testminiday", "testminiday2"]
    output_dir = "output"

    # Iterate over the crawls of all days in target_list
    for input_dir in target_list:
        # extract_page_features_text(crawls_root, input_dir, output_dir)
        combine_csv_files(input_dir, output_dir)


if __name__ == "__main__":
    run()
