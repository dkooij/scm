"""
Extract the text from a crawled page.
Author: Daan Kooij
Last modified: October 8th, 2021
"""

from html2text import html2text


def get_page_text(binary_data):
    try:
        page_data_str = str(binary_data)
        return html2text(page_data_str)
    except UnicodeDecodeError:
        return ""
