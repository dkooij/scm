"""
From a list of hyperlinks, select only those from a specific top-level domain.
Author: Daan Kooij
Last modified: May 18th, 2021
"""

INPUT = "output/links.txt"
OUTPUT = "output/links_nl.txt"
TLD = "nl"

with open(INPUT) as input_file:
    with open(OUTPUT, "w") as output_file:
        for link in input_file:
            website = link.strip().lower().split("://")[1].split("/")[0]
            tl_domain = website.split(".")[-1]
            if tl_domain == TLD:
                output_file.write(link)
