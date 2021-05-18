"""
From a Wikipedia dump, extract all hyperlinks.
Author: Daan Kooij
Last modified: May 18th, 2021
"""

import os
import re


INPUT = "input/nlwiki-20210401-pages-articles-multistream.xml"
OUTPUT = "output/links.txt"


def is_valid_url(url):
    cut_url = url.split("://")
    return len(cut_url) >= 2 and len(cut_url[1].split(".")) >= 2


os.makedirs("output")
pattern = re.compile("(?<=[\"\\[><;=\n ])http[^|\"\\]><;\n ]*")

with open(INPUT) as input_file:
    with open(OUTPUT, "w") as output_file:
        for line in input_file:
            links = pattern.findall(line)
            for link in links:
                if is_valid_url(link):
                    output_file.write(link + "\n")
