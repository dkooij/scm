"""
Extract the text from a crawled page.
Author: Daan Kooij
Last modified: November 11th, 2021
"""

import ast
from html2text import HTML2Text


def get_page_text(binary_data, one_line=True, wrap_text=False):
    try:
        page_data_str = str(binary_data, encoding="utf-8")
        parser = HTML2Text()
        parser.ignore_links = True
        parser.ignore_emphasis = True
        parser.ignore_images = True
        parser.ignore_tables = True
        if not wrap_text:
            parser.body_width = 10485760
        page_text = parser.handle(page_data_str)
        if one_line:
            words = page_text.strip().split()
            return " ".join(words)
        else:
            return page_text.splitlines()
    except UnicodeDecodeError:
        print("except UnicodeDecodeError")
    except NotImplementedError:
        print("except NotImplementedError")
    return ""


def str_list_to_list(str_list):
    return ast.literal_eval(str_list)


def list_to_str(list_arg):
    page_text = "\n".join(list_arg)
    words = page_text.strip().split()
    return " ".join(words)


def str_list_to_str(str_list):
    return list_to_str(str_list_to_list(str_list))
