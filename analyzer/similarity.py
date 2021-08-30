"""
Compute (cosine) distance between two vectors.
Author: Daan Kooij
Last modified: August 30th, 2021
"""

from scipy.spatial import distance


def compute_distance(vector_one, vector_two):
    return distance.cosine(vector_one, vector_two)
