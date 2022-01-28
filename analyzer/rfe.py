"""
Investigate how model performance decreases as certain features are dropped.
Author: Daan Kooij
Last modified: January 28th, 2022
"""

from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import RFE


feature_values = []
targets = []

with open("inputmisc/static-training-pairs-single-day.csv") as file:
    for line in file:
        parts = line.split(",")
        feature_values.append([int(x) for x in parts[1:10]])
        targets.append(int(parts[10]))

rf = RandomForestClassifier(n_estimators=20, random_state=42)

feature_names = ["Email links", "External outlinks", "Images", "Internal outlinks",
                  "Metas", "Page text", "Scripts", "Tables", "Tags"]
feature_values_cut, feature_names_cut = feature_values, feature_names
num_features = len(feature_values_cut[0])

while num_features >= 1:
    rf.fit(feature_values_cut, targets)
    print(str(num_features) + " features score: " + str(round(rf.score(feature_values_cut, targets), 4)) +
          " (selected features: " + str(feature_names_cut) + ")")

    if num_features > 1:
        rfe = RFE(rf, n_features_to_select=num_features - 1)
        rfe.fit(feature_values_cut, targets)
        ranking = rfe.ranking_
        feature_values_cut = [[x[0] for x in zip(v, ranking) if x[1] == 1] for v in feature_values_cut]
        feature_names_cut = [x[0] for x in zip(feature_names_cut, ranking) if x[1] == 1]
        num_features = len(feature_values_cut[0])
    else:
        break
