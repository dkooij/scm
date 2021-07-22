"""
HTML Detector.
Author: Daan Kooij
Last modified: July 22nd, 2021
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
                print("??? " + filepath)
    except UnicodeDecodeError:
        print("error with " + filepath)
