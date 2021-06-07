"""
Web Crawler downloader module.
Author: Daan Kooij
Last modified: June 4th, 2021
"""

import requests
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException, InvalidSessionIdException
import time

from request_status import RequestStatus


BROWSER_PATH = "/home/s1839047/firefox-headless/firefox/firefox"
GECKODRIVER_PATH = "/home/s1839047/firefox-headless/drivers/geckodriver"
EXTENSION_PATH = "/home/s1839047/firefox-headless/extensions/jid1-KKzOGWgsW3Ao4Q@jetpack.xpi"
LOG_PATH = "/dev/null"


browsers = dict()


def download_simple(url, timeout=10):
    content = None

    try:
        response = requests.get(url, allow_redirects=True, timeout=timeout)
        content = response.content
        # http_status = response.status_code
        request_status = RequestStatus.SIMPLE_SUCCESS
    except requests.ReadTimeout:
        request_status = RequestStatus.SIMPLE_TIMEOUT
    except requests.RequestException:
        request_status = RequestStatus.SIMPLE_ERROR

    return request_status, content, "wb"


def download_headless(url, tid, attempts=3):
    headless_status, headless_content = download_headless_attempt(url, tid)

    # If the headless request results in a timeout
    if headless_status == RequestStatus.HEADLESS_TIMEOUT:
        # Do a simple request to check if it is a true timeout
        simple_status, simple_content, _ = download_simple(url, timeout=5)

        # If the simple request does result in a success
        if simple_status == RequestStatus.SIMPLE_SUCCESS:
            # Try to perform the headless request again
            for _ in range(1, attempts):
                close_browser(tid)  # But first restart the browser instance (maybe it is broken)
                headless_status, headless_content = download_headless_attempt(url, tid)

                # If the headless request now does not result in a timeout
                if headless_status != RequestStatus.HEADLESS_TIMEOUT:
                    return headless_status, headless_content, "w"  # Yay, a success, we can stop now

            # If our headless re-attempts were still in vain, use the simple request results
            return simple_status, simple_content, "wb"

    # If we get here, conclude that the headless request yielded true results
    return headless_status, headless_content, "w"


def download_headless_attempt(url, tid):
    content = None

    browser = get_browser(tid)
    try:
        browser.get(url)
        content = browser.page_source
        browser.delete_all_cookies()
        request_status = RequestStatus.HEADLESS_SUCCESS
    except TimeoutException:
        request_status = RequestStatus.HEADLESS_TIMEOUT
    except WebDriverException:
        request_status = RequestStatus.HEADLESS_ERROR

    return request_status, content


def get_browser(tid):
    if tid in browsers:
        browser = browsers[tid]
    else:
        options = webdriver.FirefoxOptions()
        options.headless = True
        while True:
            try:
                browser = webdriver.Firefox(firefox_binary=BROWSER_PATH, executable_path=GECKODRIVER_PATH,
                                            service_log_path=LOG_PATH, options=options)
                break  # Browser successfully initialized!
            except TimeoutException:
                # Unable to initialize browser, likely due to do OS resources being temporarily unavailable.
                # Try again in 5 seconds.
                print("ERROR during browser initialization (thread " + tid + ")")
                time.sleep(5)
        browser.set_page_load_timeout(10)
        browser.implicitly_wait(10)
        browser.install_addon(EXTENSION_PATH)
        browsers[tid] = browser
    return browser


def close_browser(tid):
    if tid in browsers:
        try:
            browsers[tid].quit()
        except InvalidSessionIdException:
            print("ERROR cannot close browser (invalid session ID)")
        del browsers[tid]


def close_browsers():
    for tid in list(browsers.keys()):
        close_browser(tid)
