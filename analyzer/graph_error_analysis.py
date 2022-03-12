"""
Plot Random Forest prediction accuracy across the feature space.
Note: a large part of the code below is directly copied from Matplotlib documentation:
  https://matplotlib.org/stable/gallery/images_contours_and_fields/image_annotated_heatmap.html
Author: Daan Kooij
Last modified: March 12th, 2022
"""

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

import global_vars


pocket_accuracies = np.array([[0.81, 0.81, 0.81, 0.81, 0.81, 0.81, 0.81, 0.81, 0.7486, 0.737],
                              [0.928, 0.928, 0.9148, 0.8364, 0.7932, 0.7473, 0.7058, 0.6999, 0.7172, 0.7196],
                              [0.9318, 0.9318, 0.9071, 0.8651, 0.8164, 0.7449, 0.707, 0.6761, 0.6756, 0.7245],
                              [0.9694, 0.9262, 0.8889, 0.8561, 0.7995, 0.744, 0.7025, 0.6827, 0.6681, 0.6828],
                              [0.9717, 0.8767, 0.8696, 0.817, 0.7878, 0.7446, 0.7232, 0.7076, 0.7098, 0.7338],
                              [0.9769, 0.9467, 0.9049, 0.8524, 0.8028, 0.7299, 0.6692, 0.6425, 0.6772, 0.7082],
                              [0.9824, 0.9501, 0.8762, 0.8054, 0.7869, 0.7594, 0.7237, 0.7122, 0.6871, 0.694],
                              [0.7874, 0.7874, 0.7874, 0.7874, 0.7874, 0.7874, 0.7874, 0.7874, 0.7855, 0.8296],
                              [0.9785, 0.9561, 0.9048, 0.8763, 0.798, 0.7245, 0.6584, 0.6451, 0.6559, 0.7123]])

features = global_vars.FEATURE_MAP
labels = [str(i * 10) + "% - " + str((i + 1) * 10) + "%" for i in range(10)]


# The code below is copied and adjusted from
# https://matplotlib.org/stable/gallery/images_contours_and_fields/image_annotated_heatmap.html

def heatmap(data, row_labels, col_labels, ax=None,
            cbar_kw={}, cbarlabel="", **kwargs):
    """
    Create a heatmap from a numpy array and two lists of labels.

    Parameters
    ----------
    data
        A 2D numpy array of shape (M, N).
    row_labels
        A list or array of length M with the labels for the rows.
    col_labels
        A list or array of length N with the labels for the columns.
    ax
        A `matplotlib.axes.Axes` instance to which the heatmap is plotted.  If
        not provided, use current axes or create a new one.  Optional.
    cbar_kw
        A dictionary with arguments to `matplotlib.Figure.colorbar`.  Optional.
    cbarlabel
        The label for the colorbar.  Optional.
    **kwargs
        All other arguments are forwarded to `imshow`.
    """

    if not ax:
        ax = plt.gca()

    # Plot the heatmap
    im = ax.imshow(data, **kwargs)

    # Create colorbar
    cbar = ax.figure.colorbar(im, ax=ax, **cbar_kw)
    cbar.ax.set_ylabel(cbarlabel, rotation=-90, va="bottom")

    # Show all ticks and label them with the respective list entries.
    ax.set_xticks(np.arange(data.shape[1]))
    ax.set_xticklabels(col_labels)
    ax.set_yticks(np.arange(data.shape[0]))
    ax.set_yticklabels(row_labels)

    plt.xlabel("Feature percentile →")
    plt.title("Random Forest prediction accuracy across the feature space")

    # Let the horizontal axes labeling appear on top.
    ax.tick_params(top=False, bottom=True,
                   labeltop=False, labelbottom=True)

    # Rotate the tick labels and set their alignment.
    plt.setp(ax.get_xticklabels(), rotation=90)

    # Turn spines off and create white grid.
    ax.spines[:].set_visible(False)

    ax.set_xticks(np.arange(data.shape[1]+1)-.5, minor=True)
    ax.set_yticks(np.arange(data.shape[0]+1)-.5, minor=True)
    ax.grid(which="minor", color="w", linestyle='-', linewidth=3)
    ax.tick_params(which="minor", bottom=False, left=False)

    return im, cbar


def annotate_heatmap(im, data=None, valfmt="{x:.2f}",
                     textcolors=("black", "white"),
                     threshold=None, **textkw):
    """
    A function to annotate a heatmap.

    Parameters
    ----------
    im
        The AxesImage to be labeled.
    data
        Data used to annotate.  If None, the image's data is used.  Optional.
    valfmt
        The format of the annotations inside the heatmap.  This should either
        use the string format method, e.g. "$ {x:.2f}", or be a
        `matplotlib.ticker.Formatter`.  Optional.
    textcolors
        A pair of colors.  The first is used for values below a threshold,
        the second for those above.  Optional.
    threshold
        Value in data units according to which the colors from textcolors are
        applied.  If None (the default) uses the middle of the colormap as
        separation.  Optional.
    **kwargs
        All other arguments are forwarded to each call to `text` used to create
        the text labels.
    """

    if not isinstance(data, (list, np.ndarray)):
        data = im.get_array()

    # Normalize the threshold to the images color range.
    if threshold is not None:
        threshold = im.norm(threshold)
    else:
        threshold = im.norm(data.max())/2.

    # Set default alignment to center, but allow it to be
    # overwritten by textkw.
    kw = dict(horizontalalignment="center",
              verticalalignment="center")
    kw.update(textkw)

    # Get the formatter in case a string is supplied
    if isinstance(valfmt, str):
        valfmt = matplotlib.ticker.StrMethodFormatter(valfmt)

    # Loop over the data and create a `Text` for each "pixel".
    # Change the text's color depending on the data.
    texts = []
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            kw.update(color=textcolors[int(im.norm(data[i, j]) > threshold)])
            text = im.axes.text(j, i, valfmt(data[i, j] * 100, None), **kw)
            texts.append(text)

    return texts


fig, ax = plt.subplots()

im, cbar = heatmap(pocket_accuracies, features, labels, ax=ax,
                   cmap="YlGn", cbarlabel="Prediction accuracy")
texts = annotate_heatmap(im, valfmt="{x:.0f}%")

fig.tight_layout()
plt.savefig("figures/ml-static/rf-error-analysis.png", dpi=400)
