"""
Visualize how predictions by ML models are made by plotting predictions for all values in a grid.
Author: Daan Kooij
Last modified: February 19th, 2022
"""

from matplotlib.colors import ListedColormap
import matplotlib.pyplot as plt
import random

import global_vars


INPUT_DIR = "inputmisc/grid2"


def load_training_pair_data(sample_size):
    features0, features1 = [], []

    with open("inputmisc/static-training-pairs-combined-2-sample.csv") as file:
        for line in file:
            parts = line.split(",")
            features = [int(x) for x in parts[1:10]]
            target = int(parts[10])
            if target == 0:
                features0.append(features)
            else:
                features1.append(features)

    random.seed(42)
    features0_sample = random.sample(features0, int(sample_size / 2))
    features1_sample = random.sample(features1, int(sample_size / 2))

    data_sample = []
    for f0, f1 in zip(features0_sample, features1_sample):
        data_sample.append((f0, 0))
        data_sample.append((f1, 1))
    return data_sample


def draw_single_heatmap(model_name, feature1, feature2, feature_statistics, figure=None):
    xss, yss, zss = [], [], []

    with open(INPUT_DIR + "/" + model_name + "-" + str(feature1) + "-" + str(feature2) + ".csv/part-00000") as file:
        prev_x, prev_y, prev_z = -1, -1, -1
        for line in file:
            parts = line.split(",")
            x, y, z = int(parts[0]), int(parts[1]), float(parts[2])
            if x > prev_x:
                xs, ys, zs = [], [], []
                xss.append(xs)
                yss.insert(0, ys)
                zss.append(zs)
            xs.append(x)
            ys.append(y)
            zs.append(z)
            prev_x, prev_y, prev_z = x, y, z

    if figure is None:
        fig = plt.figure()
        fig.tight_layout()
    else:
        fig = figure

    fig.contourf(xss, yss, zss, cmap=plt.cm.RdBu, vmin=0.0, vmax=1.0)
    fig.set_xlim(left=xss[0][0], right=xss[-1][-1])
    fig.set_ylim(bottom=yss[0][0], top=yss[-1][-1])

    median_f1, median_f2 = feature_statistics[feature1][2], feature_statistics[feature2][2]
    fig.set_xscale("symlog", linthresh=max(median_f1, 1))
    fig.set_yscale("symlog", linthresh=max(median_f2, 1))
    fig.plot([median_f1, median_f1], [feature_statistics[feature2][0], feature_statistics[feature2][1]],
             color="black", linewidth=3.0, linestyle=(0, (3, 2)))
    fig.plot([feature_statistics[feature1][0], feature_statistics[feature1][1]], [median_f2, median_f2],
             color="black", linewidth=3.0, linestyle=(0, (3, 2)))


def draw_heatmaps(model_name, feature_pairs, rows, columns, feature_statistics, tpd=None, figure=None):
    if figure is None:
        fig = plt.figure(figsize=(3.2 * columns, 2.4 * rows))
    else:
        fig = figure

    plot_index = 0
    for x, y in feature_pairs:
        plot_index += 1
        if x != y:
            ax = fig.add_subplot(rows, columns, plot_index)
            plt.xlabel(global_vars.FEATURE_MAP[x] + " →")
            plt.ylabel(global_vars.FEATURE_MAP[y] + " →")
            if figure is not None:
                ax.get_xaxis().set_ticks([])
                ax.get_yaxis().set_ticks([])
            draw_single_heatmap(model_name, x, y, feature_statistics, figure=ax)
            if tpd is not None:
                xs = [t[0][x] for t in tpd]
                ys = [t[0][y] for t in tpd]
                cs = [t[1] for t in tpd]  # One minus the target value because colormap is opposite.
                ax.scatter(xs, ys, c=cs, s=2.0, cmap=ListedColormap(["red", "blue"]), vmin=0, vmax=1, zorder=10)

    fig.suptitle("Visualisation of decisions made by approximate " + str.upper(model_name) + " models")
    fig.tight_layout()
    if figure is None:
        fig.savefig("figures/ml-static/" + model_name + "-heatmaps-important.png", dpi=400)


def draw_all_heatmaps(model_name, feature_statistics, tpd=None):
    fig = plt.figure(figsize=(19.2, 14.4))
    feature_pairs = [(x, y) for x in range(9) for y in range(9)]
    draw_heatmaps(model_name, feature_pairs, 9, 9, feature_statistics, tpd=tpd, figure=fig)
    fig.tight_layout()
    fig.savefig("figures/ml-static/" + model_name + "-heatmaps-all.png", dpi=300)


_tpd = load_training_pair_data(1000)
_fs = [(0, 3, 0), (0, 76, 5), (0, 86, 6), (0, 339, 41), (0, 37, 9),
       (2, 792, 120), (0, 65, 14), (0, 8, 0), (6, 2186, 368)]
# draw_single_heatmap("rf", 5, 8, _fs)
draw_heatmaps("rf", [(8, 5), (8, 3), (8, 6), (5, 3), (5, 6), (3, 6)], 2, 3, _fs, tpd=_tpd)
draw_all_heatmaps("rf", _fs)
