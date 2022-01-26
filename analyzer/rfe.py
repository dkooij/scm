"""
Investigate how model performance decreases as certain features are dropped.
Author: Daan Kooij
Last modified: January 26th, 2022
"""

from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import RFE


features = []
targets = []

with open("inputmisc/static-training-pairs-single-day.csv") as file:
    for line in file:
        parts = line.split(",")
        features.append([int(x) for x in parts[1:10]])
        targets.append(int(parts[10]))


rf = RandomForestClassifier(n_estimators=20, random_state=42)

num_features = len(features[0])
features_cut = [v for v in features]
while num_features >= 1:
    rf.fit(features_cut, targets)
    print(str(num_features) + " features: " + str(rf.score(features_cut, targets)))

    if num_features > 1:
        rfe = RFE(rf, n_features_to_select=num_features - 1)
        rfe.fit(features_cut, targets)
        ranking = rfe.ranking_
        features_cut = [[x[0] for x in zip(v, ranking) if x[1] == 1] for v in features_cut]
        num_features = len(features_cut[0])
    else:
        break
