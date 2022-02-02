"""
Visualize how predictions by ML models are made by plotting predictions for all values in a grid.
Author: Daan Kooij
Last modified: February 2nd, 2022
"""

import matplotlib.pyplot as plt
import random

import global_vars


INPUT_DIR = "inputmisc/grid"


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


def draw_single_heatmap(model_name, feature1, feature2, figure=None):
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

    fig.contourf(xss, yss, zss, cmap=plt.cm.RdBu, vmin=0.25, vmax=0.75)
    fig.set_xlim(left=xss[0][0], right=xss[-1][-1])
    fig.set_ylim(bottom=yss[0][0], top=yss[-1][-1])


def draw_heatmaps(model_name, feature_pairs, rows, columns, tpd=None, figure=None):
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
            draw_single_heatmap(model_name, x, y, figure=ax)
            if tpd is not None:
                xs = [t[0][x] for t in tpd]
                ys = [t[0][y] for t in tpd]
                cs = [t[1] for t in tpd]
                ax.scatter(xs, ys, c=cs, s=0.4, cmap=plt.cm.RdBu, vmin=0, vmax=1)

    fig.suptitle("Visualisation of decisions made by " + model_name + " model")
    fig.tight_layout()
    if figure is None:
        fig.savefig("figures/ml-static/heatmaps_important.png", dpi=400)


def draw_all_heatmaps(model_name, tpd=None):
    fig = plt.figure(figsize=(19.2, 14.4))
    feature_pairs = [(x, y) for x in range(9) for y in range(9)]
    draw_heatmaps(model_name, feature_pairs, 9, 9, tpd=tpd, figure=fig)
    fig.tight_layout()
    fig.savefig("figures/ml-static/heatmaps_all.png", dpi=300)


# _tpd = load_training_pair_data(1000)
# draw_single_heatmap("rf", 5, 8)
draw_heatmaps("rf", [(8, 5), (8, 6), (8, 2), (5, 6), (5, 2), (6, 2)], 2, 3)
draw_all_heatmaps("rf")
