"""
CSV Reader Generator.
Make CSV rows indexable by CSV header keys.
Author: Daan Kooij
Last modified: August 30th, 2021
"""

import csv
import os


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

def get_log_paths(input_dir):
    log_paths = []
    for log_filename in os.listdir(input_dir):
        log_path = input_dir + "/" + log_filename
        if is_csv(log_path):
            log_paths.append(log_path)
    return log_paths


def get_log_entries(log_path):
    for log_entry in read_csv(log_path):
        if should_use_page(log_entry):
            yield log_entry


def get_all_log_entries(input_dir):
    for log_path in get_log_paths(input_dir):
        for log_entry in get_log_entries(log_path):
            yield log_entry


def get_filename(log_entry):
    return log_entry["Stage file"] + "-" + log_entry["URL index"]


def get_filepath(log_entry, input_dir):
    return input_dir + "/pages/" + get_filename(log_entry)


def should_use_page(log_entry):
    return log_entry["File present"] == "True" and \
           log_entry["Status code"] == "RequestStatus.HEADLESS_SUCCESS"
