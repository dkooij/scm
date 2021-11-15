"""
Read feature change data and compute feature change statistics.
Author: Daan Kooij
Last modified: November 15th, 2021
"""

import ast
from collections import defaultdict
import matplotlib.pyplot as plt

import csv_reader


def compute_change_fractions(differences_file_path):
    def compute(csv_path):
        count_dict = defaultdict(int)
        total = 0
        for log_entry in csv_reader.get_log_entries(csv_path, ignore_validity_check=True):
            any_change, change_detected = False, False
            for k, v in log_entry.items():
                if k != "Stage file" and k != "URL index":
                    t = ast.literal_eval(v)
                    if isinstance(t, tuple) and (t[1] != 0 or t[2] != 0):
                        count_dict[k] += 1
                        change_detected = True
                    elif k == "page_hash" and v == "1":
                        count_dict[k] += 1
                        any_change = True
            if any_change and not change_detected:
                count_dict["other"] += 1
            total += 1
        return dict((k, v / total) for (k, v) in count_dict.items())

    fractions = compute(differences_file_path)
    fractions_sorted = sorted(fractions.items(), key=lambda t: t[1], reverse=True)
    return fractions_sorted


def compute_change_amplitudes(differences_file_path):
    def compute(csv_path):
        ins_fractions, del_fractions, changes_dict = defaultdict(int), defaultdict(int), defaultdict(int)
        for log_entry in csv_reader.get_log_entries(csv_path, ignore_validity_check=True):
            for k, v in log_entry.items():
                if k != "Stage file" and k != "URL index":
                    t = ast.literal_eval(v)
                    if isinstance(t, tuple) and (t[1] != 0 or t[2] != 0):
                        total_ops = t[0] + t[1] + t[2]
                        ins_fractions[k] += t[1] / total_ops
                        del_fractions[k] += t[2] / total_ops
                        changes_dict[k] += 1
        return dict((k, v / changes_dict[k]) for (k, v) in ins_fractions.items()), \
               dict((k, v / changes_dict[k]) for (k, v) in del_fractions.items())

    insert_fractions, delete_fractions = compute(differences_file_path)
    return list(insert_fractions.items()), list(delete_fractions.items())


def plot_change_fractions(change_fractions):
    plt.bar([k for k, _ in change_fractions], [v for _, v in change_fractions])
    plt.title("Probabilities that page features change in 24 hours")
    plt.xlabel("Page feature")
    plt.ylabel("Fraction changed")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()


def plot_change_amplitudes(fractions):
    (insert_fractions, delete_fractions) = fractions
    plt.bar([k for k, _ in insert_fractions], [v for _, v in insert_fractions], color=plt.cm.Dark2(0))
    plt.bar([k for k, _ in delete_fractions], [-v for _, v in delete_fractions], color=plt.cm.Dark2(1))
    plt.title("Change amplitudes when features change")
    plt.xlabel("Page feature")
    plt.ylabel("Change amplitude")
    plt.xticks(rotation=90)
    plt.grid()
    plt.tight_layout()
    plt.show()


# plot_change_fractions(compute_change_fractions("output/differences.csv"))
plot_change_amplitudes(compute_change_amplitudes("output/differences.csv"))
