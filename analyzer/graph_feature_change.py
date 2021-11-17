"""
Read feature change data and compute feature change statistics.
Author: Daan Kooij
Last modified: November 17th, 2021
"""

import ast
from collections import defaultdict
import matplotlib.pyplot as plt

import csv_reader


def compute_change_fractions(differences_file_path):
    def compute(csv_path):
        count_dict = defaultdict(int)
        informative_list = [0, 0, 0, 0]
        informative_features = ("text", "internal_outlinks", "external_outlinks", "email_links", "images", "tables")
        total = 0
        for log_entry in csv_reader.get_log_entries(csv_path, ignore_validity_check=True):
            any_change, change_detected = False, False
            informative_bool, not_informative_bool = False, False
            for k, v in log_entry.items():
                if k != "Stage file" and k != "URL index":
                    t = ast.literal_eval(v)
                    if isinstance(t, tuple) and (t[1] != 0 or t[2] != 0):
                        count_dict[k] += 1
                        change_detected = True
                        if k in informative_features:
                            informative_bool = True
                        else:
                            not_informative_bool = True
                    elif k == "page_hash" and v == "1":
                        any_change = True
                        # count_dict[k] += 1
            # if any_change and not change_detected:
            #     count_dict["other"] += 1
            if any_change:
                informative_list[2 * informative_bool + not_informative_bool] += 1
            total += 1
        return dict((k, v / total) for (k, v) in count_dict.items()), tuple(reversed(informative_list))

    fractions, informative_tuple = compute(differences_file_path)
    fractions_sorted = sorted(fractions.items(), key=lambda t: t[1], reverse=True)
    return fractions_sorted, informative_tuple


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
    sorted_fractions = sorted(list(zip(insert_fractions.items(), delete_fractions.items())),
                              key=lambda t: t[0][1] + t[1][1], reverse=True)
    return [t[0] for t in sorted_fractions], [t[1] for t in sorted_fractions]


def plot_change_fractions(change_fractions):
    plt.figure()
    plt.bar([k for k, _ in change_fractions], [v for _, v in change_fractions], color=plt.cm.Dark2(0))
    plt.title("Probabilities that page features change in 24 hours")
    plt.xlabel("Page feature")
    plt.ylabel("Fraction changed")
    plt.xticks(rotation=90)
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/change-fractions.png", dpi=400)
    print(" * plotted change fractions")


def plot_informative_tuple(informative_tuple):
    plt.figure()
    labels = ("Both", "Informative", "Not informative", "Neither")
    plt.pie(informative_tuple, labels=labels, normalize=True, startangle=90, counterclock=False,
            colors=plt.cm.Dark2.colors)
    plt.title("Fraction of changed pages for which page changes are likely informative")
    plt.tight_layout()
    plt.savefig("figures/informative-pie.png", dpi=400)
    print(" * plotted informative tuple")


def plot_change_amplitudes(fractions):
    (insert_fractions, delete_fractions) = fractions
    plt.figure()
    plt.bar([k for k, _ in insert_fractions], [v for _, v in insert_fractions], color=plt.cm.Dark2(0))
    plt.bar([k for k, _ in delete_fractions], [-v for _, v in delete_fractions], color=plt.cm.Dark2(1))
    plt.title("Change amplitudes when features change")
    plt.xlabel("Page feature")
    plt.ylabel("Change amplitude")
    plt.xticks(rotation=90)
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/change-amplitudes.png", dpi=400)
    print(" * plotted change amplitudes")


_change_fractions, _informative_tuple = compute_change_fractions("output/differences.csv")
print("Change fractions:", _change_fractions)
print("Informative tuple:", _informative_tuple)
plot_change_fractions(_change_fractions)
plot_informative_tuple(_informative_tuple)

_change_amplitudes = compute_change_amplitudes("output/differences.csv")
plot_change_amplitudes(_change_amplitudes)
