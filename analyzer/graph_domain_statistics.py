"""
Visualize relative sizes of crawl stages in a graph.
Author: Daan Kooij
Last modified: April 22nd, 2022
"""

import matplotlib.pyplot as plt


def draw_relative_stage_sizes(percentages):
    plt.figure(figsize=(6.4, 3.2))
    x = list(range(1, len(percentages) + 1))
    plt.plot(x, percentages, linewidth=2.5, color=plt.cm.Dark2(0))
    plt.title("Relative size of crawl stages")
    plt.xlabel("Crawl stage index →")
    plt.ylabel("Relative stage size (%) →")
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/data/relative-stage-size.png", dpi=400)


_percentages = [100.0, 32.76, 18.79, 13.51, 10.71, 8.95, 7.66, 6.78, 6.06, 5.5,
                5.04, 4.58, 4.27, 4.02, 3.8, 3.59, 3.38, 3.21, 3.07, 2.97,
                2.85, 2.75, 2.65, 2.57, 2.48, 2.41, 2.34, 2.28, 2.21, 2.15,
                2.11, 2.05, 2.0, 1.94, 1.89, 1.84, 1.8, 1.74, 1.7, 1.68,
                1.64, 1.62, 1.59, 1.56, 1.52, 1.49, 1.47, 1.44, 1.41, 1.39]
draw_relative_stage_sizes(_percentages)
