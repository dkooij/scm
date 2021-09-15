"""
HTML Detector.
Author: Daan Kooij
Last modified: September 15th, 2021
"""

from bs4 import BeautifulSoup


def get_html(file):
    # Requires file in binary-reading mode
    page_html = BeautifulSoup(file, "html.parser")
    if page_html.find("html"):
        return page_html
    return False


def get_page_text(page_html):
    words = []

    for p in page_html.find_all("p"):
        line_words = p.get_text().strip().split()
        words.extend(line_words)

    return " ".join(words), words
