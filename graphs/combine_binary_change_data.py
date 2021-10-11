"""
Combine binary change data into a single csv file.
Author: Daan Kooij
Last modified: October 11th, 2021
"""

import os


BINARY_CHANGE_ROOT = "input/change-rate"
OUTPUT_FILE_PATH = "input/change-data-combined.csv"


def binary_change_entries():
    for change_dir in os.listdir(BINARY_CHANGE_ROOT):
        change_dir_path = BINARY_CHANGE_ROOT + "/" + change_dir
        if os.path.isdir(change_dir_path):

            for change_file_name in os.listdir(change_dir_path):
                if change_file_name.startswith("part-"):
                    change_file_path = change_dir_path + "/" + change_file_name

                    with open(change_file_path) as file:
                        for line in file:
                            yield line.strip().split(",")


with open(OUTPUT_FILE_PATH, "w") as output_file:
    for [page_id, day1, day2, changed_str] in binary_change_entries():
        page_id_short = page_id.split("_")[-1]
        changed_short = "1" if changed_str == "True" else "0"
        output_line = page_id_short + "," + day2 + "," + changed_short
        output_file.write(output_line + "\n")
