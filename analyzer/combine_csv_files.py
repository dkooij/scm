"""
Combine a collection of CSV files into a single CSV file.
Author: Daan Kooij
Last modified: December 17th, 2021
"""

import os


INPUT_DIRECTORY = "inputmisc/static-training-pairs"
OUTPUT_DIRECTORY = "inputmisc/static-training-pairs-combined.csv"


def csv_entries():
    for directory in os.listdir(INPUT_DIRECTORY):
        directory_path = INPUT_DIRECTORY + "/" + directory
        if os.path.isdir(directory_path):

            for file_name in os.listdir(directory_path):
                if file_name.startswith("part-"):
                    csv_file_path = directory_path + "/" + file_name

                    with open(csv_file_path) as file:
                        for line in file:
                            yield line


with open(OUTPUT_DIRECTORY, "w") as output_file:
    for csv_entry in csv_entries():
        output_file.write(csv_entry + "\n")
