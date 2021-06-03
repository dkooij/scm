"""
Request Status enum.
Author: Daan Kooij
Last modified: June 3rd, 2021
"""

from enum import Enum


class RequestStatus(Enum):
    SIMPLE_SUCCESS = 100
    SIMPLE_TIMEOUT = 101
    SIMPLE_ERROR = 102

    HEADLESS_SUCCESS = 200
    HEADLESS_TIMEOUT = 201
    HEADLESS_ERROR = 202
