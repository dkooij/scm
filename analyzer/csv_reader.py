"""
CSV Reader Generator.
Make CSV rows indexable by CSV header keys.
Author: Daan Kooij
Last modified: August 5th, 2021
"""

import csv


INPUT_DIR = "input"


def is_csv(filepath):
    return filepath.endswith(".csv")


def read_csv(filepath):
    with open(filepath) as log_file:
        log_reader = csv.reader(log_file)
        header_row = next(log_reader, None)
        for log_row in log_reader:
            yield dict(zip(header_row, log_row))


def should_use_page(log_entry):
    return log_entry["File present"] == "True" and \
           log_entry["Status code"] == "RequestStatus.HEADLESS_SUCCESS"


def get_filepath(log_entry):
    return INPUT_DIR + "/pages/" + log_entry["Stage file"] + "-" + log_entry["URL index"]
