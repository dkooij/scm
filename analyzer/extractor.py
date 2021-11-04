"""
Feature Extractor.
Author: Daan Kooij
Last modified: November 4th, 2021
"""

from datetime import datetime
import os

import csv_reader
from data_point import DataPoint
import detect_html
from protocol import Protocol
from weekday import Weekday


# Control functions

def extract_static_features(log_entry, page_html, input_dir=None, page_words=None, data_point=None):
    if data_point is None:
        data_point = DataPoint()

    # Meta features
    set_meta_features(log_entry, data_point, input_dir=input_dir)

    # URL features
    set_url_features(log_entry, data_point)

    # Linkage features
    set_linkage_features(log_entry, page_html, data_point)

    # HTML features
    set_html_features(page_html, data_point)

    # Text features
    if page_words is None:
        _, page_words = detect_html.get_page_text(page_html)
    set_text_features(page_words, data_point)

    return data_point


# Log entry functions

def get_timestamp(log_entry):
    timestamp_str = log_entry["Timestamp"]
    return datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")


def get_url(log_entry):
    return log_entry["URL"]


# URL helper functions

def get_sl_domain(url):
    # Example: "https://a.b.c.nl/d/e/f" returns "c"
    try:
        return get_root_domain(url).rsplit(".", 2)[-2]
    except IndexError:
        return get_root_domain(url)


def get_root_domain(url):
    # Example: "https://a.b.c.nl/d/e/f" returns "a.b.c.nl"
    try:
        return strip_protocol(url).split("/", 1)[0]
    except IndexError:
        return strip_protocol(url)


def strip_protocol(url):
    while True:
        if url.startswith("https://"):
            url = url[8:]
        elif url.startswith("http://"):
            url = url[7:]
        else:
            return url


def get_url_subdirs(url):
    return strip_protocol(url).split("/")[1:]


def get_url_subdomains(url):
    url_before_subdirs = strip_protocol(url).split("/", 1)[0]
    return url_before_subdirs.split(".")[:-2]


def href_to_url(href_url, root_domain, current_url):
    # Remove trailing "/"
    href_url = href_url.rstrip("/")

    if len(href_url) == 0:
        return current_url
    elif href_url.startswith("http://") or href_url.startswith("https://"):
        return strip_protocol(href_url)
    elif href_url.startswith("/"):
        return root_domain + href_url
    else:
        return current_url + "/" + href_url


# Meta feature extraction functions

def set_meta_features(log_entry, data_point, input_dir=None):
    data_point.set_feature("size", get_file_size(log_entry, input_dir=input_dir))
    data_point.set_feature("weekday", get_weekday(log_entry))


def get_file_size(log_entry, input_dir=None):
    if input_dir is None:
        return len(log_entry["Binary data"])
    else:
        file_path = csv_reader.get_filepath(log_entry, input_dir)
        return os.path.getsize(file_path)


def get_weekday(log_entry):
    timestamp = get_timestamp(log_entry)
    weekday_int = timestamp.weekday()
    return Weekday(weekday_int)


# URL feature extraction functions

def set_url_features(log_entry, data_point):
    data_point.set_feature("url_length", get_url_length(log_entry))
    data_point.set_feature("domain_name_length", get_domain_name_length(log_entry))
    data_point.set_feature("domain_name_digits", get_domain_name_digits(log_entry))
    data_point.set_feature("domain_name_special_chars", get_domain_name_special_chars(log_entry))
    data_point.set_feature("url_subdir_depth", get_url_subdir_depth(log_entry))
    data_point.set_feature("url_subdomain_depth", get_url_subdomain_depth(log_entry))
    data_point.set_feature("protocol", get_protocol(log_entry))


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
    internal_outlinks, external_outlinks, email_links = get_raw_linkage_features(log_entry, page_html)

    data_point.set_feature("internal_outlinks", len(internal_outlinks))
    data_point.set_feature("external_outlinks", len(external_outlinks))
    data_point.set_feature("email_links", len(email_links))


def get_raw_linkage_features(log_entry, page_html):
    current_url = strip_protocol(get_url(log_entry))
    root_domain = get_root_domain(current_url)

    internal_outlinks, external_outlinks, email_links = [], [], []
    for a in page_html.find_all("a", href=True):
        href_url = a["href"].strip()
        if href_url.startswith("mailto:"):
            email_links.append(href_url)
        else:
            link = href_to_url(href_url, root_domain, current_url)
            if get_sl_domain(link) == get_sl_domain(current_url):
                internal_outlinks.append(link)
            else:
                external_outlinks.append(link)
    return internal_outlinks, external_outlinks, email_links


# HTML feature extraction functions

def set_html_features(page_html, data_point):
    images, tables, scripts, metas, tags = get_raw_html_features(page_html)

    # Store the computed HTML features in the data point
    data_point.set_feature("images", len(images))
    data_point.set_feature("tables", len(tables))
    data_point.set_feature("scripts", len(scripts))
    data_point.set_feature("meta", len(metas))
    data_point.set_feature("tags_total", len(tags))
    data_point.set_feature("tags_unique", len(set(tags)))


def get_raw_html_features(page_html):
    # Retrieve images, tables, scripts, meta properties, and HTML tags
    images, tables, scripts, metas, tags = [], [], [], [], []
    for image in page_html.find_all("img"):
        images.append(str(image))
    for table in page_html.find_all("table"):
        tables.append(str(table))
    for script in page_html.find_all("script"):
        scripts.append(str(script))
    for meta in page_html.find_all("meta"):
        metas.append(str(meta))
    for tag in page_html.find_all():
        tags.append(tag.name)
    return images, tables, scripts, metas, tags


# Text feature extraction functions

def set_text_features(page_words, data_point):
    # Count number of words (total and unique)
    words_set, words_total = set(), 0
    for word in page_words:
        lowercase_word = word.lower()
        words_total += 1
        words_set.add(lowercase_word)
    words_unique = len(words_set)

    # Store the computed text features in the data point
    data_point.set_feature("words_total", words_total)
    data_point.set_feature("words_unique", words_unique)


# Semantic feature extraction functions

# def set_semantic_features(pca_model, log_entry, data_point, compute_device):
#     tensor_filename = csv_reader.get_filename(log_entry) + ".pt"
#     tensor_filepath = TENSOR_EMBEDDINGS_PATH + "/" + tensor_filename
#     if os.path.isfile(tensor_filepath):
#         tensor = torch.load(tensor_filepath, map_location=compute_device)
#         semantic_features = pca_model.transform([tensor.tolist()])[0].tolist()
#         data_point.set_feature("page_content", semantic_features)
#     else:
#         print("ERROR: unable to load semantic embeddings for " + tensor_filename)
