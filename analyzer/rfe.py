"""
Investigate how model performance decreases as certain features are dropped.
Author: Daan Kooij
Last modified: January 31st, 2022
"""

from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import RFE
from sklearn.model_selection import train_test_split


feature_values = []
targets = []

with open("inputmisc/static-training-pairs-single-day.csv") as file:
    for line in file:
        parts = line.split(",")
        feature_values.append([int(x) for x in parts[1:10]])
        targets.append(int(parts[10]))

rf = RandomForestClassifier(min_samples_leaf=10, n_estimators=100, random_state=42)

feature_names = ["Email links", "External outlinks", "Images", "Internal outlinks",
                  "Metas", "Page text", "Scripts", "Tables", "Tags"]

features_train, features_test, targets_train, targets_test = train_test_split(feature_values, targets, random_state=42)

features_train_cut, features_test_cut, feature_names_cut = features_train, features_test, feature_names
num_features = len(features_train_cut[0])

while num_features >= 1:
    rf.fit(features_train_cut, targets_train)
    print(str(num_features) + " features score: " + str(round(rf.score(features_test_cut, targets_test), 4)) +
          " (selected features: " + str(feature_names_cut) + ")")

    if num_features > 1:
        rfe = RFE(rf, n_features_to_select=num_features - 1)
        rfe.fit(features_train_cut, targets_train)
        ranking = rfe.ranking_
        features_train_cut = [[x[0] for x in zip(v, ranking) if x[1] == 1] for v in features_train_cut]
        features_test_cut = [[x[0] for x in zip(v, ranking) if x[1] == 1] for v in features_test_cut]
        feature_names_cut = [x[0] for x in zip(feature_names_cut, ranking) if x[1] == 1]
        num_features = len(features_train_cut[0])
    else:
        break
