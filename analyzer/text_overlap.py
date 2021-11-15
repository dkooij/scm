"""
Compute the overlap between two lists of texts.
Author: Daan Kooij
Last modified: November 12th, 2021
"""

import ast
import difflib
import os
import random

import csv_reader


def get_overlap_fraction(value1_list, value2_list):
    return difflib.SequenceMatcher(None, value1_list, value2_list).ratio()


def start_overlap(value1_list, value2_list, start_length=3):
    return value1_list[:start_length] == value2_list[:start_length]


def get_opcode_stats(value1_list, value2_list):
    op_list = difflib.SequenceMatcher(None, value1_list, value2_list).get_opcodes()
    equal, insert, delete = 0, 0, 0
    for (tag, i1, i2, j1, j2) in op_list:
        if tag == "equal":
            equal += i2 - i1
        elif tag == "insert":
            insert += j2 - j1
        elif tag == "delete":
            delete += i2 - i1
        elif tag == "replace":
            insert += j2 - j1
            delete += i2 - i1
    return equal, insert, delete


def compute_change(input1_dir, input2_dir, output_dir, name, include_equal=True):
    entry_path1 = output_dir + "/" + input1_dir + "/combined/" + name + ".csv"
    entry_path2 = output_dir + "/" + input2_dir + "/combined/" + name + ".csv"
    equal, insert, delete = 0, 0, 0
    for log_entry_pair in csv_reader.get_log_entry_pairs(entry_path1, entry_path2, ignore_validity_check=True):
        value1_str_list, value2_str_list = log_entry_pair[name + "-1"], log_entry_pair[name + "-2"]
        value1_list, value2_list = ast.literal_eval(value1_str_list), ast.literal_eval(value2_str_list)
        t = get_opcode_stats(value1_list, value2_list)
        if include_equal or t[1] != 0 or t[2] != 0:
            equal += t[0]
            insert += t[1]
            delete += t[2]
    return equal, insert, delete


def html_compare(input1_dir, input2_dir, output_dir, name, sample=-1):
    entry_path1 = output_dir + "/" + input1_dir + "/combined/" + name + ".csv"
    entry_path2 = output_dir + "/" + input2_dir + "/combined/" + name + ".csv"
    output_root = "outputhtml/" + name
    os.makedirs(output_root, exist_ok=True)

    log_entry_pairs = csv_reader.get_log_entry_pairs(entry_path1, entry_path2, ignore_validity_check=True)

    def random_change_sample(entries_gen):
        entries_list = [e for e in entries_gen if e[name + "-1"] != e[name + "-2"]]
        random.seed(42)  # To allow reproduction of results
        return random.sample(entries_list, sample)

    if sample >= 1:
        log_entry_pairs = random_change_sample(log_entry_pairs)

    for log_entry_pair in log_entry_pairs:
        value1_str_list, value2_str_list = log_entry_pair[name + "-1"], log_entry_pair[name + "-2"]
        value1_list, value2_list = ast.literal_eval(value1_str_list), ast.literal_eval(value2_str_list)
        if value1_list != value2_list:
            html_diff = difflib.HtmlDiff()
            html_source = html_diff.make_file(value1_list, value2_list)
            output_path = output_root + "/" + log_entry_pair["Stage file"] + "-" + log_entry_pair["URL index"] + ".html"
            with open(output_path, "w") as output_file:
                output_file.write(html_source)


# print(compute_change("testminiday", "testminiday2", "outputmini", "text"))
# html_compare("testminiday", "testminiday2", "outputmini", "text", sample=10)
# html_compare("20210612", "20210613", "output", "text", sample=300)
