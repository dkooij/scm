"""
Compute the overlap between two lists of texts.
Author: Daan Kooij
Last modified: November 23rd, 2021
"""

import difflib


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
