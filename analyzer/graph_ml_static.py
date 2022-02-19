"""
Draw graphs based on results of static Machine Learning.
Author: Daan Kooij
Last modified: February 19th, 2022
"""

import matplotlib.pyplot as plt

import global_vars


def get_rf_feature_importances():
    _feature_importances = sorted([(0, 0.020738161643856216), (1, 0.09708887956745434), (2, 0.12264212407006918),
                                   (3, 0.11588174141222238), (4, 0.10275270372390673), (5, 0.17964717734163063),
                                   (6, 0.11581059358610776), (7, 0.024089975742814098), (8, 0.22134864291193862)],
                                  key=lambda t: t[1], reverse=True)
    return [(global_vars.FEATURE_MAP[k], v) for k, v in _feature_importances]


def get_lr_regression_coefficients():
    _regression_coefficients = sorted([(0, 0.017799054624042927), (1, 0.0032078416789266388),
                                       (2, 0.009474672853599495), (3, 0.0016879880079477072),
                                       (4, 0.043010053175781146), (5, -0.00015456534136614575),
                                       (6, 0.018545431011434115),(7, -0.006648655035289558),
                                       (8, 0.00022509766872622422)], key=lambda t: abs(t[1]), reverse=True)
    return [(global_vars.FEATURE_MAP[k], v) for k, v in _regression_coefficients]


def get_feature_subset_performance():
    return [59.72, 62.93, 69.05, 72.7, 75.39, 77.02, 78.24, 78.75, 78.51]


def plot_rf_feature_importances(feature_importances):
    plt.figure()
    plt.bar([k for k, _ in feature_importances], [v for _, v in feature_importances], color=plt.cm.Dark2(0))
    plt.title("Feature importances according to Random Forest model")
    plt.xlabel("Feature →")
    plt.ylabel("Importance →")
    plt.xticks(rotation=90)
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/ml-static/rf-feature-importances.png", dpi=400)
    print(" * plotted rf feature importances")


def plot_lr_regression_coefficients(regression_coefficients):
    plt.figure()
    plt.bar([k for k, _ in regression_coefficients], [v for _, v in regression_coefficients],
            color=[plt.cm.Dark2(0 if v >= 0 else 1) for _, v in regression_coefficients])
    plt.title("Regression coefficients according to Logistic Regression model")
    plt.xlabel("Feature →")
    plt.ylabel("Regression coefficient →")
    plt.xticks(rotation=90)
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/ml-static/lr-regression-coefficients.png", dpi=400)
    print(" * plotted lr regression coefficients")


def plot_feature_subset_performance(subset_performances):
    plt.figure(figsize=(6.4, 3.2))
    plt.plot(list(range(1, len(subset_performances) + 1)), subset_performances, linewidth=2.5, color=plt.cm.Dark2(0))
    plt.title("Performance of RF model when trained with $\\it{n}$ top features")
    plt.xlabel("$\\it{n}$ →")
    plt.ylabel("Minimum recall →")
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/ml-static/feature-subset-performance.png", dpi=400)
    print(" * plotted feature subset performance")


plot_rf_feature_importances(get_rf_feature_importances())
plot_lr_regression_coefficients(get_lr_regression_coefficients())
plot_feature_subset_performance(get_feature_subset_performance())
