"""
Compute domain statistics based on crawl lists.
Author: Daan Kooij
Last modified: April 22nd, 2022
"""

import os


STAGES_DIR = "output/stages"


stage_index, first_stage_urls = 0, 0
num_urls_list, percentages = [], []

for stage_filename in sorted(os.listdir(STAGES_DIR)):
    stage_path = STAGES_DIR + "/" + stage_filename
    num_urls = 0
    stage_index += 1
    with open(stage_path) as stage_file:
        for url in stage_file:
            num_urls += 1
    first_stage_urls = max(first_stage_urls, num_urls)

    num_urls_list.append(num_urls)
    percentages.append(round(100 * num_urls / first_stage_urls, 2))

for stage_index, num_urls, percentage in zip(range(1, len(num_urls_list) + 1), num_urls_list, percentages):
    print("Stage " + str(stage_index) + " urls: " + str(num_urls) + " (" + str(percentage) + "%)")

print("\n" + str(percentages))
