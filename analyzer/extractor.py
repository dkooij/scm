"""
Feature Extractor.
Author: Daan Kooij
Last modified: July 27th, 2021
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
                    with open(get_filepath(log_entry)) as file:
                        page_html = detect_html.get_html(file)
                        if page_html:  # If the HTML can be parsed successfully
                            data_point = extract_features(log_entry, page_html)
                            data_points.append(data_point)

    return data_points


def extract_features(log_entry, page_html):
    data_point = DataPoint()

    # Meta features
    data_point.set_feature("size", get_file_size(log_entry))
    data_point.set_feature("weekday", get_weekday(log_entry))

    # URL features
    data_point.set_feature("url_length", get_url_length(log_entry))
    data_point.set_feature("domain_name_length", get_domain_name_length(log_entry))
    data_point.set_feature("domain_name_digits", get_domain_name_digits(log_entry))
    data_point.set_feature("domain_name_special_chars", get_domain_name_special_chars(log_entry))
    data_point.set_feature("url_subdir_depth", get_url_subdir_depth(log_entry))
    data_point.set_feature("url_subdomain_depth", get_url_subdomain_depth(log_entry))
    data_point.set_feature("protocol", get_protocol(log_entry))

    # Linkage features
    set_linkage_features(log_entry, page_html, data_point)

    # HTML features
    set_html_features(log_entry, page_html, data_point)

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
    # Example: "https://a.b.c.nl/d/e/f" returns "c"
    return get_root_domain(url).rsplit(".", 2)[-2]


def get_root_domain(url):
    # Example: "https://a.b.c.nl/d/e/f" returns "a.b.c.nl"
    return strip_protocol(url).split("/", 1)[0]


def strip_protocol(url):
    return url.split("://")[-1]


def get_url_subdirs(url):
    url_after_protocol = url.split("://")[1]
    return url_after_protocol.split("/")[1:]


def get_url_subdomains(url):
    url_after_protocol = url.split("://")[1]
    url_before_subdirs = url_after_protocol.split("/", 1)[0]
    return url_before_subdirs.split(".")[:-2]


def href_to_url(href_url, root_domain, current_url):
    if href_url.startswith("http://") or \
            href_url.startswith("https://"):
        return strip_protocol(href_url)
    elif href_url.startswith("/"):
        return root_domain + href_url
    else:
        return current_url + "/" + href_url


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


# Linkage feature extraction functions

def set_linkage_features(log_entry, page_html, data_point):
    current_url = strip_protocol(get_url(log_entry))
    root_domain = get_root_domain(current_url)

    internal_outlinks, external_outlinks, mailto_links = 0, 0, 0
    for a in page_html.find_all("a", href=True):
        href_url = a["href"].strip()
        if href_url.startswith("mailto:"):
            mailto_links += 1
        else:
            link = href_to_url(href_url, root_domain, current_url)
            if get_sl_domain(link) == get_sl_domain(current_url):
                internal_outlinks += 1
            else:
                external_outlinks += 1

    data_point.set_feature("internal_outlinks", internal_outlinks)
    data_point.set_feature("external_outlinks", external_outlinks)
    data_point.set_feature("email_links", mailto_links)


# HTML feature extraction functions

def set_html_features(log_entry, page_html, data_point):
    # Count number of images, tables, scripts, and meta properties
    images, tables, scripts, meta = 0, 0, 0, 0
    for _ in page_html.find_all("img"):
        images += 1
    for _ in page_html.find_all("table"):
        tables += 1
    for _ in page_html.find_all("script"):
        scripts += 1
    for _ in page_html.find_all("meta"):
        meta += 1

    # Count number of tags (total and unique)
    tags_set, tags_total = set(), 0
    for tag in page_html.find_all():
        tags_total += 1
        tags_set.add(tag.name)
    tags_unique = len(tags_set)

    # Store the computed HTML features in the data point
    data_point.set_feature("images", images)
    data_point.set_feature("tables", tables)
    data_point.set_feature("scripts", scripts)
    data_point.set_feature("meta", meta)
    data_point.set_feature("tags_total", tags_total)
    data_point.set_feature("tags_unique", tags_unique)


# Invoke base function
dps = get_data_points()
