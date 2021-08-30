"""
Detect the validity of page crawls.
In some scenarios, the Web crawler stores a duplicate version
  of a previous page under the name of the current page.
This module detects such false duplicate crawled pages.
Author: Daan Kooij
Last modified: August 30th, 2021
"""

import csv_reader
from data_point import DataPoint
import extractor
import similarity


DISTANCE_THRESHOLD = 0.01


def detect_duplicates():
    pca_model = extractor.load_pca_model()
    for log_path in csv_reader.get_log_paths():
        previous_semantic_vector = None
        for log_entry in csv_reader.get_log_entries(log_path):
            data_point = DataPoint()
            extractor.set_semantic_features(pca_model, log_entry, data_point)
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

            print(log_entry["URL index"], page_valid)


detect_duplicates()
