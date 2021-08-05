"""
CSV Reader Generator.
Make CSV rows indexable by CSV header keys.
Author: Daan Kooij
Last modified: August 5th, 2021
"""

import csv
import os


INPUT_DIR = "input"


# Generic CSV reader functions

def is_csv(filepath):
    return filepath.endswith(".csv")


def read_csv(filepath):
    with open(filepath) as log_file:
        log_reader = csv.reader(log_file)
        header_row = next(log_reader, None)
        for log_row in log_reader:
            yield dict(zip(header_row, log_row))


# Crawler-specific CSV reader functions

def get_log_entries():
    for log_filename in os.listdir(INPUT_DIR):
        log_path = INPUT_DIR + "/" + log_filename

        if is_csv(log_path):
            for log_entry in read_csv(log_path):
                if should_use_page(log_entry):
                    yield log_entry


def get_filepath(log_entry):
    return INPUT_DIR + "/pages/" + log_entry["Stage file"] + "-" + log_entry["URL index"]


def should_use_page(log_entry):
    return log_entry["File present"] == "True" and \
           log_entry["Status code"] == "RequestStatus.HEADLESS_SUCCESS"
