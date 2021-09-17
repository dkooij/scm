"""
Import to allow imports from super directory.
Author: Daan Kooij
Last modified: September 17th, 2021
"""

import os
import sys


sys.path.append(os.path.abspath(os.path.join("../")))
