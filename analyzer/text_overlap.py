"""
Compute the overlap between two lists of texts.
Author: Daan Kooij
Last modified: November 11th, 2021
"""

import ast
import difflib

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

        # if text1 != text2:
        #     h = difflib.HtmlDiff()
        #     d = h.make_file(text1, text2)
        #     with open("test.html", "w") as file:
        #         file.write(d)
        #         print()
            # x = get_overlap_fraction(text1, text2)
            # print(x)


# print(compute_change("testminiday", "testminiday2", "outputmini", "text"))
# print(compute_change("testminiday", "testminiday2", "outputmini", "text", include_equal=False))
