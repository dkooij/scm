"""
Read feature change data and compute feature change statistics.
Author: Daan Kooij
Last modified: November 22nd, 2021
"""

import ast
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np

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
        ins_fractions_lists, del_fractions_lists = defaultdict(list), defaultdict(list)
        for log_entry in csv_reader.get_log_entries(csv_path, ignore_validity_check=True):
            for k, v in log_entry.items():
                if k != "Stage file" and k != "URL index":
                    t = ast.literal_eval(v)
                    if isinstance(t, tuple) and (t[1] != 0 or t[2] != 0):
                        total_ops = t[0] + t[1] + t[2]
                        ins_fractions[k] += t[1] / total_ops
                        del_fractions[k] += t[2] / total_ops
                        ins_fractions_lists[k].append(t[1] / total_ops)
                        del_fractions_lists[k].append(t[2] / total_ops)
                        changes_dict[k] += 1
        return dict((k, v / changes_dict[k]) for (k, v) in ins_fractions.items()), \
               dict((k, v / changes_dict[k]) for (k, v) in del_fractions.items()), \
               ins_fractions_lists, del_fractions_lists

    insert_fractions, delete_fractions, ins_fractions_lists, del_fractions_lists = compute(differences_file_path)
    sorted_fractions = sorted(list(zip(insert_fractions.items(), delete_fractions.items())),
                              key=lambda t: t[0][1] + t[1][1], reverse=True)

    change_amplitudes = ([t[0] for t in sorted_fractions], [t[1] for t in sorted_fractions])
    fractions_lists = sorted([(k, (sorted(ins_fractions_lists[k], reverse=True),
                                   sorted(del_fractions_lists[k], reverse=True)))
                              for k in ins_fractions_lists.keys()])

    return change_amplitudes, fractions_lists


def get_text_change_categories(csv_file_path):
    entries = csv_reader.get_log_entries(csv_file_path, ignore_validity_check=True, excel_mode=True)
    parsed_entries = map(lambda e: {"Page": e["Page"], "Categories": set(e["Categories"].split(","))}, entries)

    categories = defaultdict(int)
    number_of_entries = 0
    for entry in parsed_entries:
        for category in entry["Categories"]:
            categories[category] += 1
        number_of_entries += 1

    categories_list = sorted(list(categories.items()), key=lambda t: (-t[1], t[0]))
    categories_list = [(c, a / number_of_entries) for c, a in categories_list]
    return categories_list


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


def plot_change_amplitude_percentile(fractions_lists):
    fig = plt.figure()  # plt.figure(figsize=(6.4, 4.8))
    plot_index = 0
    for name, (y_ins, y_del) in fractions_lists:
        plot_index += 1
        ax = fig.add_subplot(3, 3, plot_index)
        x = np.linspace(0, 1, num=len(y_ins))
        ax.plot(x, y_del, color=plt.cm.Dark2(1), linewidth=2.5, label="del")
        ax.plot(x, y_ins, color=plt.cm.Dark2(0), linewidth=2.5, label="ins")
        ax.set_title(name)
    fig.suptitle("Change amplitude percentile plots per feature")
    plt.tight_layout()

    plt.savefig("figures/change-amplitudes-percentile.png", dpi=400)
    print(" * plotted change amplitudes percentile curve")


def plot_text_change_categories(categories_list):
    plt.figure()
    plt.bar([k for k, _ in categories_list], [v for _, v in categories_list], color=plt.cm.Dark2(0))
    plt.title("Distribution over text change categories")
    plt.xlabel("Change category")
    plt.ylabel("Fraction of cases")
    plt.xticks(rotation=90)
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/text-change-categories.png", dpi=400)
    print(" * plotted text change categories")


_change_fractions, _informative_tuple = compute_change_fractions("output/differences.csv")
print("Change fractions:", _change_fractions)
print("Informative tuple:", _informative_tuple)
plot_change_fractions(_change_fractions)
plot_informative_tuple(_informative_tuple)

_change_amplitudes, _fractions_lists = compute_change_amplitudes("output/differences.csv")
plot_change_amplitudes(_change_amplitudes)
plot_change_amplitude_percentile(_fractions_lists)

plot_text_change_categories(get_text_change_categories("inputmisc/manual-change-categories.csv"))
