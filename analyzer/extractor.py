"""
Feature Extractor.
Author: Daan Kooij
Last modified: July 23rd, 2021
"""

from datetime import datetime
import os

from analyzer.weekday import Weekday
import csv_reader
from data_point import DataPoint
import detect_html


INPUT_DIR = "input"


# Control functions

def get_data_points():
    data_points = []

    for log_filename in os.listdir(INPUT_DIR):
        log_path = INPUT_DIR + "/" + log_filename

        if csv_reader.is_csv(log_path):
            for log_entry in csv_reader.read_csv(log_path):
                if should_use_page(log_entry):
                    with open(get_filepath(log_entry)) as raw_page:
                        if detect_html.is_html(raw_page):
                            data_point = extract_features(log_entry, raw_page)
                            data_points.append(data_point)

    return data_points


def extract_features(log_entry, raw_page):
    data_point = DataPoint()

    data_point.set_feature("size", get_file_size(log_entry))
    data_point.set_feature("weekday", get_weekday(log_entry))

    return data_point


# Log entry functions

def should_use_page(log_entry):
    return log_entry["File present"] == "True" and \
           log_entry["Status code"] == "RequestStatus.HEADLESS_SUCCESS"


def get_filepath(log_entry):
    return INPUT_DIR + "/pages/" + log_entry["Stage file"] + "-" + log_entry["URL index"]


def get_timestamp(log_entry):
    timestamp_str = log_entry["Timestamp"]
    return datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")


def get_url(log_entry):
    return log_entry["URL"]


# Meta feature extraction functions

def get_file_size(log_entry):
    file_path = get_filepath(log_entry)
    return os.path.getsize(file_path)


def get_weekday(log_entry):
    timestamp = get_timestamp(log_entry)
    weekday_int = timestamp.weekday()
    return Weekday(weekday_int)


# Invoke base function
dps = get_data_points()
