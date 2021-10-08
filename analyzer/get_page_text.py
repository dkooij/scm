"""
Extract the text from a crawled page.
Author: Daan Kooij
Last modified: October 8th, 2021
"""

from html2text import HTML2Text


def get_page_text(binary_data):
    try:
        page_data_str = str(binary_data, encoding="utf-8")
        parser = HTML2Text()
        parser.ignore_links = True
        parser.ignore_emphasis = True
        parser.ignore_images = True
        parser.ignore_tables = True
        page_text = parser.handle(page_data_str)
        words = page_text.strip().split()
        return " ".join(words)
    except UnicodeDecodeError:
        print("except UnicodeDecodeError")
    except NotImplementedError:
        print("except NotImplementedError")
    return ""
