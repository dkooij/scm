"""
Compute the overlap between two lists of texts.
Author: Daan Kooij
Last modified: November 11th, 2021
"""

import ast
import difflib

import csv_reader


def get_overlap_fraction(text1, text2):
    return difflib.SequenceMatcher(None, text1, text2).ratio()


def start_overlap(text1, text2, start_length=3):
    return text1[:start_length] == text2[:start_length]


def compute_change(input1_dir, input2_dir, output_dir):
    entry_path1 = output_dir + "/" + input1_dir + "/combined/text.csv"
    entry_path2 = output_dir + "/" + input2_dir + "/combined/text.csv"
    for log_entry_pair in csv_reader.get_log_entry_pairs(entry_path1, entry_path2, ignore_validity_check=True):
        text1_list, text2_list = log_entry_pair["text-1"], log_entry_pair["text-2"]
        text1, text2 = ast.literal_eval(text1_list), ast.literal_eval(text2_list)
        if text1 != text2:
            h = difflib.HtmlDiff()
            d = h.make_file(text1, text2)
            with open("test.html", "w") as file:
                file.write(d)
                print()
            # x = get_overlap_fraction(text1, text2)
            # print(x)


# compute_change("testminiday", "testminiday2", "outputmini")
