"""
HTML Detector.
Author: Daan Kooij
Last modified: July 26th, 2021
"""

from bs4 import BeautifulSoup


def get_html(file):
    try:
        page_html = BeautifulSoup(file, "html.parser")
        if page_html.find():
            return page_html
    except UnicodeDecodeError:
        pass
    return False
