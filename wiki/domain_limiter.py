"""
From a list of hyperlinks, select at most a certain amount from each second-level domain prioritised by link popularity.
Author: Daan Kooij
Last modified: June 4th, 2021
"""

from collections import defaultdict
import os
import random


INPUT = "output/links_nl.txt"
OUTPUT = "output/links_nl_limited.txt"
OUTPUT_STAGES = "output/stages"
SELECTION_METHOD = 0  # 0: random selection, 1: prioritise popular links
SLD_LIMIT = 100

link_dict = defaultdict(lambda: defaultdict(int))
stage_dict = defaultdict(list)

with open(INPUT) as input_file:
    for link in input_file:
        website = link.strip().lower().split("://")[1].split("/")[0]
        website_parts = website.split(".")
        sl_domain = website_parts[-2] + "." + website_parts[-1]
        link_dict[sl_domain][link] += 1

random.seed("EMOTION")
with open(OUTPUT, "w") as output_file:
    for sl_domain, links_dict in link_dict.items():
        if SELECTION_METHOD == 0:
            links = list(links_dict.keys())
            selected_links = random.sample(links, min(SLD_LIMIT, len(links)))
        else:
            links_popularity = sorted(links_dict.items(), key=lambda x: x[1], reverse=True)
            selected_links = [link for (link, _) in links_popularity[:SLD_LIMIT]]
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
