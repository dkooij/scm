"""
CSV Reader Generator.
Make CSV rows indexable by CSV header keys.
Author: Daan Kooij
Last modified: July 23rd, 2021
"""

import csv


def read_csv(filepath):
    with open(filepath) as log_file:
        log_reader = csv.reader(log_file)
        header_row = next(log_reader, None)
        for log_row in log_reader:
            yield dict(zip(header_row, log_row))
