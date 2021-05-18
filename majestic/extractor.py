"""
From a Majestic Million csv file, extract all second-level domains from a specific top-level domain.
Author: Daan Kooij
Last modified: May 18th, 2021
"""

import csv
import os


INPUT = "input/majestic_million.csv"
OUTPUT = "output/links.txt"
TLD = "nl"

if not os.path.exists("output"):
    os.mkdir("output")

with open(INPUT) as input_file:
    csv_reader = csv.reader(input_file)
    with open(OUTPUT, "w") as output_file:
        for row in csv_reader:
            if row[3] == TLD:
                output_file.write(row[2] + "\n")
