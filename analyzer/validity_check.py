"""
Detect the validity of page crawls.
In some scenarios, the Web crawler stores a duplicate version
  of a previous page under the name of the current page.
This module detects such false duplicate crawled pages.
Author: Daan Kooij
Last modified: September 2nd, 2021
"""

import hashlib

import csv_reader
from data_point import DataPoint
import detect_html
import embedding
import extractor
import similarity


DISTANCE_THRESHOLD = 0.01


def detect_duplicates_semantic():
    # This function is deprecated!
    # It is incorrect in the sense that it concludes
    # consecutive pages are equal (invalid) if they both have no text;
    # i.e., page html features are not taken into account in that case
    # (they are taken into account at the detect_duplicates_overlap function).

    pca_model = extractor.load_pca_model()
    compute_device = embedding.get_compute_device()
    for log_path in csv_reader.get_log_paths():
        previous_semantic_vector = None
        for log_entry in csv_reader.get_log_entries(log_path):
            data_point = DataPoint()
            extractor.set_semantic_features(pca_model, log_entry, data_point, compute_device)
            try:
                semantic_vector = data_point.get_feature("page_content")
            except KeyError:
                semantic_vector = None

            if semantic_vector is None:
                page_valid = False
            else:  # If semantic_vector is not None
                if previous_semantic_vector is None:
                    page_valid = True
                else:  # If previous_semantic_vector is not None
                    distance = similarity.compute_distance(previous_semantic_vector, semantic_vector)
                    if distance >= DISTANCE_THRESHOLD:
                        page_valid = True
                    else:
                        page_valid = False

                previous_semantic_vector = semantic_vector

            if not page_valid:
                print(log_entry["Stage file"] + "-" + log_entry["URL index"])


def detect_duplicates_overlap():
    for log_path in csv_reader.get_log_paths():
        previous_page_text_hash, previous_data_point = None, None
        for log_entry in csv_reader.get_log_entries(log_path):

            with open(csv_reader.get_filepath(log_entry), "rb") as file:
                page_html = detect_html.get_html(file)
                if page_html:  # If the HTML can be parsed successfully
                    page_text = embedding.get_page_text(page_html)

                    if len(page_text) == 0:
                        # If the page contains no "text", then check if the page
                        # is the same as the previous one by comparing HTML features.
                        data_point = DataPoint()
                        extractor.set_html_features(page_html, data_point)
                        page_valid = previous_data_point != data_point
                        previous_page_text_hash = None, data_point
                    else:
                        # If the page contains "text", then check if the page
                        # is the same as the previous one by comparing the MD5 hash of the text.
                        # (this is the most frequent case)
                        page_text_hash = hashlib.md5(page_text.encode()).digest()
                        page_valid = previous_page_text_hash != page_text_hash
                        previous_page_text_hash, previous_data_point = page_text_hash, None

                else:  # If the HTML cannot be parsed
                    page_valid = False

            if not page_valid:
                print(log_entry["Stage file"] + "-" + log_entry["URL index"])


# detect_duplicates_semantic()
# detect_duplicates_overlap()
