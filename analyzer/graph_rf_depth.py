"""
Plot Random Forest prediction performance for different maximum tree depths.
Author: Daan Kooij
Last modified: January 24th, 2022
"""

import matplotlib.pyplot as plt


def plot_rf_depth_performance(tree_depths, minimum_recalls):
    plt.figure()
    plt.plot(tree_depths, minimum_recalls, linewidth=2.5, color=plt.cm.Dark2(0))
    plt.title("Influence of RF maximum tree depth on minimum prediction recall")
    plt.xlabel("Tree depth")
    plt.ylabel("Minimum recall")
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/ml-static/rf-depth-performance.png", dpi=400)


_tree_depths = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
_minimum_recalls = [56.52, 62.37, 64.69, 66.24, 67.54, 69.07, 70.31, 71.26, 72.56, 73.46, 74.59, 76.15, 77.18, 78.11]
plot_rf_depth_performance(_tree_depths, _minimum_recalls)
