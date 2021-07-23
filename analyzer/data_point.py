"""
Data Point class.
Author: Daan Kooij
Last modified: July 23rd, 2021
"""


class DataPoint:

    def __init__(self):
        self._features = dict()

    def set_feature(self, feature_name, feature_value):
        self._features[feature_name] = feature_value

    def get_feature(self, feature_name):
        return self._features[feature_name]
