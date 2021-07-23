"""
Feature Extractor.
Author: Daan Kooij
Last modified: July 23rd, 2021
"""

from datetime import datetime
import os

from analyzer.protocol import Protocol
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
    data_point.set_feature("url_length", get_url_length(log_entry))
    data_point.set_feature("domain_name_length", get_domain_name_length(log_entry))
    data_point.set_feature("domain_name_digits", get_domain_name_digits(log_entry))
    data_point.set_feature("domain_name_special_chars", get_domain_name_special_chars(log_entry))
    data_point.set_feature("url_subdir_depth", get_url_subdir_depth(log_entry))
    data_point.set_feature("url_subdomain_depth", get_url_subdomain_depth(log_entry))
    data_point.set_feature("protocol", get_protocol(log_entry))

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


# URL helper functions

def get_sl_domain(url):
    url_after_protocol = url.split("://")[1]
    url_before_subdirs = url_after_protocol.split("/", 1)[0]
    return url_before_subdirs.rsplit(".", 2)[-2]


def get_url_subdirs(url):
    url_after_protocol = url.split("://")[1]
    return url_after_protocol.split("/")[1:]


def get_url_subdomains(url):
    url_after_protocol = url.split("://")[1]
    url_before_subdirs = url_after_protocol.split("/", 1)[0]
    return url_before_subdirs.split(".")[:-2]


# Meta feature extraction functions

def get_file_size(log_entry):
    file_path = get_filepath(log_entry)
    return os.path.getsize(file_path)


def get_weekday(log_entry):
    timestamp = get_timestamp(log_entry)
    weekday_int = timestamp.weekday()
    return Weekday(weekday_int)


# URL feature extraction functions

def get_url_length(log_entry):
    url = get_url(log_entry)
    return len(url)


def get_domain_name_length(log_entry):
    url = get_url(log_entry)
    sl_domain = get_sl_domain(url)
    return len(sl_domain)


def get_domain_name_digits(log_entry):
    url = get_url(log_entry)
    sl_domain = get_sl_domain(url)
    return sum(c.isdigit() for c in sl_domain)


def get_domain_name_special_chars(log_entry):
    url = get_url(log_entry)
    sl_domain = get_sl_domain(url)
    return sum(not (c.isdigit() or c.isalpha()) for c in sl_domain)


def get_url_subdir_depth(log_entry):
    url = get_url(log_entry)
    subdirs = get_url_subdirs(url)
    return len(subdirs)


def get_url_subdomain_depth(log_entry):
    url = get_url(log_entry)
    subdomains = get_url_subdomains(url)
    return len(subdomains)


def get_protocol(log_entry):
    url = get_url(log_entry)
    protocol_str = url.split("://")[0]
    if protocol_str.lower() == "https":
        return Protocol.HTTPS
    else:
        return Protocol.HTTP


# Invoke base function
dps = get_data_points()
