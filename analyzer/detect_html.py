"""
HTML Detector.
Author: Daan Kooij
Last modified: November 11th, 2021
"""

from bs4 import BeautifulSoup


def get_html(file):
    # Requires file in binary-reading mode
    try:
        page_html = BeautifulSoup(file, "html.parser")
        if page_html.find("html"):
            return page_html
        else:
            page_html.decompose()
    except TypeError:
        print("BS4: Error while parsing crawled page")
    return False


def get_page_text(page_html):
    raw_text = page_html.get_text()
    lines = raw_text.splitlines()

    return [x.strip() for x in lines if len(x.strip()) > 0]
