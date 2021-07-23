"""
HTML Detector.
Author: Daan Kooij
Last modified: July 23rd, 2021
"""

from bs4 import BeautifulSoup
import os


INPUT_DIR = "input"


for filename in sorted(os.listdir(INPUT_DIR)):
    filepath = INPUT_DIR + "/" + filename
    try:
        with open(filepath) as file:
            x = bool(BeautifulSoup(file, "html.parser").find())
            if not x:
                print("no html " + filepath)
    except UnicodeDecodeError:
        print("error with " + filepath)


def is_html(file):
    try:
        return bool(BeautifulSoup(file, "html.parser").find())
    except UnicodeDecodeError:
        return False
