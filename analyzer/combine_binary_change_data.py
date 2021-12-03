"""
Combine binary change data into a single csv file.
Author: Daan Kooij
Last modified: December 1st, 2021
"""

import os


BINARY_CHANGE_ROOT = "inputmisc/change-data"
OUTPUT_FILE_PATH = "inputmisc/change-data-combined.csv"


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
    for change_entry in binary_change_entries():
        page_id, day1, day2, change_values = change_entry[0], change_entry[1], change_entry[2], change_entry[3:]
        page_id_short = page_id.split("_")[-1]
        change_values_str = ",".join(change_values)
        output_line = ",".join((page_id_short, day2, change_values_str))
        output_file.write(output_line + "\n")
