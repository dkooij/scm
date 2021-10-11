"""
Read binary change data and compute binary change statistics.
Author: Daan Kooij
Last modified: October 11th, 2021
"""

from collections import defaultdict
from datetime import datetime


INPUT_FILE_PATH = "input/change-data-combined.csv"
FIRST_WEEK = 24
LAST_WEEK = 26


def compute_change_dict():
    change_dict = defaultdict(lambda: defaultdict(int))

    with open(INPUT_FILE_PATH) as input_file:
        for line in input_file:
            [page_id, date_str, change_str] = line.strip().split(",")
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            week = date_obj.isocalendar()[1]
            if week > LAST_WEEK:
                break  # TODO
            change_dict[page_id][week] += int(change_str)

    return change_dict


def compute_changes_per_week(change_dict):
    change_matrix = []
    for week_number in range(FIRST_WEEK, LAST_WEEK + 1):
        change_list = [0 for _ in range(8)]
        for changes_per_week_dict in change_dict.values():
            change_list[changes_per_week_dict[week_number]] += 1
        change_matrix.append(change_list)
    return change_matrix


def compute_change_cube(change_dict):
    change_cube = []  # change_cube[week_number][prev_num_changes][cur_num_changes] = occurrences
    for week_number in range(FIRST_WEEK + 1, LAST_WEEK + 1):  # Only after first week

        # change_matrix[prev_num_changes][cur_num_changes] = occurrences
        change_matrix = [[0 for _ in range(8)] for _ in range(8)]
        for page_id, changes_per_week_dict in change_dict.items():
            previous_week_changes = changes_per_week_dict[week_number - 1]
            current_week_changes = changes_per_week_dict[week_number]
            change_matrix[previous_week_changes][current_week_changes] += 1

        change_cube.append(change_matrix)

    return change_cube


cd = compute_change_dict()
cm = compute_changes_per_week(cd)
cc = compute_change_cube(cd)

print("cm:", cm)
print("cc:", cc)
