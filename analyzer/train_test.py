"""
Randomly split a collection into two (mutually exclusive, covering) subsets.
Author: Daan Kooij
Last modified: September 14th, 2021
"""

import os
from sklearn.model_selection import train_test_split

import extractor


def split_collection(collection, train_size):
    return train_test_split(collection, train_size=train_size, random_state=1492021)


def split_domains(first_stage_path, extract_root, train_test_dir, train_size):
    # First load all second level domains
    # (these can be retrieved from the first stage file,
    #  later stages cannot include new SL domains)
    all_domains = []
    with open(first_stage_path) as stage_file:
        for line in stage_file:
            all_domains.append(extractor.get_sl_domain(line.strip()))

    # Split the domains into training and testing sets
    train_set, test_set = split_collection(all_domains, train_size)

    # Prepare the output directory
    output_path = extract_root + "/" + train_test_dir
    os.makedirs(output_path, exist_ok=True)

    # Write the training and testing split to disk
    train_output_path = output_path + "/train.txt"
    with open(train_output_path, "w") as train_output_file:
        for url in train_set:
            train_output_file.write(url + "\n")

    test_output_path = output_path + "/test.txt"
    with open(test_output_path, "w") as test_output_file:
        for url in test_set:
            test_output_file.write(url + "\n")
