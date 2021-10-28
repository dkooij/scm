"""
CSV Reader Generator.
Make CSV rows indexable by CSV header keys.
Author: Daan Kooij
Last modified: October 28th, 2021
"""

import csv
import os


# CSV reader setup

csv.field_size_limit(10485760)


# Generic CSV reader functions

def is_csv(filepath):
    return filepath.endswith(".csv")


def read_csv(filepath):
    with open(filepath, encoding="utf-8") as log_file:
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


def get_log_entries(log_path, ignore_validity_check=False):
    for log_entry in read_csv(log_path):
        if ignore_validity_check or should_use_page(log_entry):
            yield log_entry


def get_all_log_entries(input_dir, ignore_validity_check=False):
    for log_path in get_log_paths(input_dir):
        for log_entry in get_log_entries(log_path, ignore_validity_check=ignore_validity_check):
            yield log_entry


def get_filename(log_entry):
    return log_entry["Stage file"] + "-" + log_entry["URL index"]


def get_filepath(log_entry, input_dir):
    return input_dir + "/pages/" + get_filename(log_entry)


def should_use_page(log_entry):
    return log_entry["File present"] == "True" and \
           log_entry["Status code"] == "RequestStatus.HEADLESS_SUCCESS"


# Write CSV files

def write_csv_file(output_path, log_entries, field_order=None):
    if field_order is None:
        field_order = ["Stage file", "URL index"]
    if len(log_entries) == 0:
        print("Error: requires log_entries to not be empty")

    with open(output_path, "w", newline="") as output_file:
        output_writer = csv.writer(output_file)
        field_list = field_order + sorted(list(set(log_entries[0].keys()) - set(field_order)))
        output_writer.writerow(field_list)
        for entry in log_entries:
            output_writer.writerow([entry[k] for k in field_list])
