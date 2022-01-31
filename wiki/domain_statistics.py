"""
Compute domain statistics based on crawl lists.
Author: Daan Kooij
Last modified: January 31st, 2022
"""

import os


STAGES_DIR = "output/stages"


stage_index, first_stage_urls = 0, 0
for stage_filename in sorted(os.listdir(STAGES_DIR)):
    stage_path = STAGES_DIR + "/" + stage_filename
    num_urls = 0
    stage_index += 1
    with open(stage_path) as stage_file:
        for url in stage_file:
            num_urls += 1
    first_stage_urls = max(first_stage_urls, num_urls)

    print("Stage " + str(stage_index) + " urls: " + str(num_urls) +
          " (" + str(round(100 * num_urls / first_stage_urls, 2)) + "%)")
