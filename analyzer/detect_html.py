"""
HTML Detector.
Author: Daan Kooij
Last modified: August 31st, 2021
"""

from bs4 import BeautifulSoup


def get_html(file):
    # Requires file in binary-reading mode
    page_html = BeautifulSoup(file, "html.parser")
    if page_html.find():
        return page_html
    return False
