"""
Data Point class.
Author: Daan Kooij
Last modified: July 23rd, 2021
"""


class DataPoint:

    def __init__(self):
        self.features = dict()

    def set_feature(self, feature_name, feature_value):
        self.features[feature_name] = feature_value

    def get_feature(self, feature_name):
        return self.features[feature_name]

    def __repr__(self):
        return str(self.features)
