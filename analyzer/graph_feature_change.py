"""
Read feature change data and compute feature change statistics.
Author: Daan Kooij
Last modified: November 4th, 2021
"""

from collections import defaultdict
import plotly.express as px

import csv_reader


def compute_change_fractions_single(csv_path):
    count_dict = defaultdict(int)
    total = 0
    for log_entry in csv_reader.get_log_entries(csv_path, ignore_validity_check=True):
        for k, v in log_entry.items():
            if k != "Stage file" and k != "URL index":
                count_dict[k] += int(v)
        total += 1
    return dict((k, v / total) for (k, v) in count_dict.items())


def compute_change_fractions():
    fractions = compute_change_fractions_single("outputmini/differences.csv")
    fractions_sorted = sorted(fractions.items(), key=lambda t: t[1], reverse=True)
    return fractions_sorted


def plot_change_fractions(change_fractions):
    px.bar(
        x=[k for k, _ in change_fractions],
        y=[v for _, v in change_fractions],
        title="Probabilities that page features change in 24 hours",
        labels={"x": "Page feature", "y": "Fraction changed"}
    ).show()


plot_change_fractions(compute_change_fractions())
