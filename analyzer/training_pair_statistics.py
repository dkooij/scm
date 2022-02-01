"""
Compute basic statistics about training pair features.
Author: Daan Kooij
Last modified: February 1st, 2022
"""


def get_minimum(value_list, ignore_extremes=0.):
    # Requires value_list to be sorted
    size = len(value_list)
    return value_list[0 + int(size * ignore_extremes)]


def get_maximum(value_list, ignore_extremes=0.):
    # Requires value_list to be sorted
    size = len(value_list)
    return value_list[-1 - int(size * ignore_extremes)]


def get_average(value_list, ignore_extremes=0.):
    # Requires value_list to be sorted
    size = len(value_list)
    if ignore_extremes > 0:
        new_size = int(size * (1 - 2 * ignore_extremes))
        return sum(value_list[int(size * ignore_extremes):-int(size * ignore_extremes)]) / new_size
    else:
        return sum(value_list) / size


def get_median(value_list):
    # Requires value_list to be sorted
    size = len(value_list)
    if size % 2 == 0:
        i1 = int(size / 2)
        i2 = i1 + 1
        return int((value_list[i1] + value_list[i2]) / 2)
    else:
        return value_list[int(size / 2)]


def get_value_statistics(value_list, ignore_extremes=0.):
    value_list.sort()
    return ("min", get_minimum(value_list, ignore_extremes=ignore_extremes)), \
           ("max", get_maximum(value_list, ignore_extremes=ignore_extremes)), \
           ("avg", get_average(value_list, ignore_extremes=ignore_extremes)), \
           ("med", get_median(value_list))


def training_pair_statistics(ignore_extremes=0.):
    values = []

    with open("inputmisc/static-training-pairs-combined-2.csv") as file:
        for line in file:
            parts = line.split(",")[1:10]
            for i, v in zip(range(len(parts)), parts):
                if i >= len(values):
                    values.append([])
                values[i].append(int(v))

    for value_list in values:
        print(get_value_statistics(value_list, ignore_extremes=ignore_extremes))


training_pair_statistics(ignore_extremes=0.025)
