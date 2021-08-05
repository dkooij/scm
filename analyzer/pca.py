"""
Reduce dimensionality of data by performing Principal Component Analysis (PCA).
Author: Daan Kooij
Last modified: August 5th, 2021
"""

import itertools
import os
import pickle
from sklearn.decomposition import PCA
import torch


INPUT_DIR = "tensor"
OUTPUT_FILE = "model/pca.skl"


def load_embeddings(indices=None):
    embeddings = []

    for i, tensor_filename in zip(itertools.count(), os.listdir(INPUT_DIR)):
        if indices is not None and i not in indices:
            continue  # Do not use this tensor for training the PCA
        tensor_path = INPUT_DIR + "/" + tensor_filename
        tensor = torch.load(tensor_path)
        embeddings.append(tensor.numpy())

    return embeddings


def train_pca(n_components=24, indices=None):
    pca = PCA(n_components=n_components)
    embeddings = load_embeddings(indices=indices)
    pca.fit(embeddings)

    # Save model to disk
    with open(OUTPUT_FILE, "wb") as file:
        pickle.dump(pca, file)


train_pca()
