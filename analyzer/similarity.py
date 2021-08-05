"""
Compute (cosine) similarity between two vectors.
Author: Daan Kooij
Last modified: August 5th, 2021
"""

from scipy.spatial import distance


def compute_distance(vector_one, vector_two):
    return distance.cosine(vector_one, vector_two)


def compute_similarity(vector_one, vector_two):
    return 1 - compute_distance(vector_one, vector_two)
