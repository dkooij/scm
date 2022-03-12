"""
Compute basic statistics about training pair features.
Author: Daan Kooij
Last modified: March 12th, 2022
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
    return ("min", get_minimum(value_list, ignore_extremes=ignore_extremes)), \
           ("max", get_maximum(value_list, ignore_extremes=ignore_extremes)), \
           ("avg", get_average(value_list, ignore_extremes=ignore_extremes)), \
           ("med", get_median(value_list))


def get_value_lists():
    value_lists = []

    with open("inputmisc/static-training-pairs-combined-2.csv") as file:
        for line in file:
            parts = line.split(",")[1:10]
            for i, v in zip(range(len(parts)), parts):
                if i >= len(value_lists):
                    value_lists.append([])
                value_lists[i].append(int(v))

    return [sorted(value_list) for value_list in value_lists]


def training_pair_statistics(ignore_extremes=0.):
    value_lists = get_value_lists()

    for value_list in value_lists:
        print(get_value_statistics(value_list, ignore_extremes=ignore_extremes))


def get_intervals(num_intervals):
    intervals = []
    value_lists = get_value_lists()
    num_values = len(value_lists[0])
    interval_size = (num_values - 1) / num_intervals
    edge_indices = [int(interval_size * i) for i in range(num_intervals + 1)]

    for value_list in value_lists:
        interval_edges = [value_list[i] for i in edge_indices]
        interval_edges[-1] += 1  # To include last value in [start, end) intervals
        intervals.append([(interval_edges[i], interval_edges[i + 1]) for i in range(num_intervals)])

    return intervals


# training_pair_statistics(ignore_extremes=0.025)
print(get_intervals(10))
