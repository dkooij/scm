"""
Compute basic distribution statistics about feature numbers.
Author: Daan Kooij
Last modified: April 22nd, 2022
"""


def get_feature_value_list_tuples(feature_names):
    feature_value_list_tuples = [(feature_name, []) for feature_name in feature_names]

    with open("inputmisc/feature-numbers-combined.csv") as file:
        for line in file:
            parts = [int(value) for value in line.split(",")[2:]]
            for feature_index, value in zip(range(len(feature_names)), parts):
                feature_value_list_tuples[feature_index][1].append(value)

    for _, value_list in feature_value_list_tuples:
        value_list.sort()

    return feature_value_list_tuples


def get_percentile_values(feature_value_list_tuples, percentiles):
    percentile_values = []

    feature_percentile_indices = [int((len(feature_value_list_tuples[0][1]) - 1) * p / 100) for p in percentiles]
    for (feature_name, value_list) in feature_value_list_tuples:
        percentile_values.append((feature_name, [value_list[i] for i in feature_percentile_indices]))

    return percentile_values


_feature_names = ["Email links", "External outlinks", "Images", "Internal outlinks",
                  "Metas", "Page text", "Scripts", "Tables", "Tags"]
_percentiles = list(range(0, 101, 20))
for _t in get_percentile_values(get_feature_value_list_tuples(_feature_names), _percentiles):
    print(_t)
