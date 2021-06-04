"""
From a list of hyperlinks, randomly select at most a certain amount from each second-level domain.
Author: Daan Kooij
Last modified: June 4th, 2021
"""

from collections import defaultdict
import os
import random


INPUT = "output/links_nl.txt"
OUTPUT = "output/links_nl_limited.txt"
OUTPUT_STAGES = "output/stages"
SLD_LIMIT = 100

link_dict = defaultdict(set)
stage_dict = defaultdict(list)

with open(INPUT) as input_file:
    for link in input_file:
        website = link.strip().lower().split("://")[1].split("/")[0]
        website_parts = website.split(".")
        sl_domain = website_parts[-2] + "." + website_parts[-1]
        link_dict[sl_domain].add(link)

random.seed("EMOTION")
with open(OUTPUT, "w") as output_file:
    for sl_domain, links in link_dict.items():
        link_list = list(links)
        if len(link_list) <= SLD_LIMIT:
            selected_links = link_list
        else:
            selected_links = random.sample(link_list, SLD_LIMIT)
        for stage, link in zip(range(SLD_LIMIT), selected_links):
            stage_dict[stage].append(link)
            output_file.write(link)

# Split into stages
os.makedirs(OUTPUT_STAGES)
n_digits = len(str(SLD_LIMIT - 1))
format_str = OUTPUT_STAGES + "/links_s{:0" + str(n_digits) + "d}.txt"
for stage, links in stage_dict.items():
    with open(format_str.format(stage), "w") as stage_file:
        for link in links:
            stage_file.write(link)
