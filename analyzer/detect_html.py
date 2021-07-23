"""
HTML Detector.
Author: Daan Kooij
Last modified: July 23rd, 2021
"""

from bs4 import BeautifulSoup


def is_html(file):
    try:
        return bool(BeautifulSoup(file, "html.parser").find())
    except UnicodeDecodeError:
        return False
