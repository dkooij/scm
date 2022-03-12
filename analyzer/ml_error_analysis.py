"""
Using predictions by a ML model, investigate in what pockets the errors are.
Author: Daan Kooij
Last modified: March 12th, 2022
"""

INTERVALS = [[(0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 1), (1, 2), (2, 648)],
             [(0, 0), (0, 0), (0, 2), (2, 3), (3, 5), (5, 7), (7, 10), (10, 16), (16, 28), (28, 6962)],
             [(0, 0), (0, 1), (1, 2), (2, 4), (4, 6), (6, 9), (9, 14), (14, 21), (21, 36), (36, 7060)],
             [(0, 1), (1, 8), (8, 18), (18, 29), (29, 41), (41, 55), (55, 74), (74, 100), (100, 161), (161, 15180)],
             [(0, 2), (2, 3), (3, 5), (5, 7), (7, 9), (9, 13), (13, 16), (16, 20), (20, 25), (25, 3270)],
             [(2, 12), (12, 47), (47, 73), (73, 100), (100, 120),
              (120, 152), (152, 193), (193, 252), (252, 374), (374, 84633)],
             [(0, 0), (0, 3), (3, 7), (7, 10), (10, 14), (14, 17), (17, 22), (22, 29), (29, 41), (41, 516)],
             [(0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 2), (2, 2509)],
             [(3, 46), (46, 154), (154, 232), (232, 297), (297, 368),
              (368, 456), (456, 567), (567, 733), (733, 1057), (1057, 85976)]]


def get_error_heatmap(num_features, num_intervals):
    correct_predictions = [[0 for _ in range(num_intervals)] for _ in range(num_features)]
    total_predictions = [[0 for _ in range(num_intervals)] for _ in range(num_features)]

    with open("inputmisc/rf-predictions-combined.csv") as file:
        for line in file:
            parts = line.strip().split(",")
            features = [int(p) for p in parts[:num_features]]
            target, predicted = int(parts[num_features]), int(parts[num_features + 1])

            for feature_index, feature_value in zip(range(num_features), features):
                feature_intervals = INTERVALS[feature_index]
                for interval_index, (interval_start, interval_end) in zip(range(num_intervals), feature_intervals):
                    interval_end = max(interval_start + 1, interval_end)  # To ensure the interval is at least one wide
                    if interval_start <= feature_value < interval_end:
                        correct_predictions[feature_index][interval_index] += target == predicted
                        total_predictions[feature_index][interval_index] += 1

    for feature_index in range(num_features):
        for interval_index in range(num_intervals):
            c, t = correct_predictions[feature_index][interval_index], total_predictions[feature_index][interval_index]
            correct_predictions[feature_index][interval_index] = round(c / t, 4)

    return correct_predictions


print(get_error_heatmap(9, 10))
