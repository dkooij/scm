"""
Reduce dimensionality of data by performing Principal Component Analysis (PCA).
Author: Daan Kooij
Last modified: August 5th, 2021
"""

import os
from sklearn.decomposition import PCA
import torch


INPUT_DIR = "tensor"


def load_embeddings():
    embeddings = []

    for tensor_filename in os.listdir(INPUT_DIR):
        tensor_path = INPUT_DIR + "/" + tensor_filename
        tensor = torch.load(tensor_path)
        embeddings.append(tensor.numpy())

    return embeddings


embeddings = load_embeddings()
pca = PCA(n_components=24)
pca.fit(embeddings)

principal_components = pca.transform(embeddings)
projections = pca.inverse_transform(principal_components)
print(sum(pca.explained_variance_ratio_))

"""
data = [[1, 0, 2, 0],
        [0, 4, 0, 3],
        [4, 0, 8, 0],
        [0, 1, 0, 0],
        [2, 3, 4, 2],
        [3, 2, 6, 1],
        [1, 1, 2, 0],
        [4, 2, 8, 1]]

pca = PCA(n_components=2)
principal_components = pca.fit_transform(data)
original = pca.inverse_transform(principal_components)
"""