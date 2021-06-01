import requests
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException

BROWSER_PATH = "/home/s1839047/firefox-headless/firefox/firefox"
EXTENSION_PATH = "/home/s1839047/firefox-headless/extensions/jid1-KKzOGWgsW3Ao4Q@jetpack.xpi"
LOG_PATH = "/dev/null"


browsers = dict()


def download_simple(url):
    status_code, content = -1, None
    try:
        response = requests.get(url, allow_redirects=True)
        status_code = response.status_code
        content = response.content
    except requests.RequestException:
        pass
    return status_code, content, "wb"


def download_headless(url, tid):
    status, content = -1, None
    browser = get_browser(tid)
    try:
        browser.get(url)
        content = browser.page_source
        status = 0
    except TimeoutException:
        print("  ERROR timeout download", tid, url)
        status = 1
    except WebDriverException:
        print("  ERROR webdriver download", tid, url)
        status = 2
    # print("finish download", tid, url)
    browser.delete_all_cookies()
    return status, content, "w"


def get_browser(tid):
    if tid in browsers:
        browser = browsers[tid]
    else:
        options = webdriver.FirefoxOptions()
        options.headless = True
        browser = webdriver.Firefox(firefox_binary=BROWSER_PATH, service_log_path=LOG_PATH, options=options)
        browser.set_page_load_timeout(10)
        browser.implicitly_wait(10)
        browser.install_addon(EXTENSION_PATH)
        browsers[tid] = browser
    return browser


def close_browser(tid):
    if tid in browsers:
        browsers[tid].close()
        del browsers[tid]


def close_browsers():
    for tid in list(browsers.keys()):
        close_browser(tid)
