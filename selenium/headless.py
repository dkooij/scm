from selenium import webdriver
import time


try:
    binary_path = "/home/s1839047/firefox-headless/firefox/firefox"
    extension_path = "/home/s1839047/firefox-headless/extensions/jid1-KKzOGWgsW3Ao4Q@jetpack.xpi"

    options = webdriver.FirefoxOptions()
    options.headless = True

    browser = webdriver.Firefox(firefox_binary=binary_path, options=options)
    browser.install_addon(extension_path)

    browser.get("https://tweakers.net/")
    time.sleep(5)  # To allow the the accept button to be clicked
    print(browser.page_source)

    browser.delete_all_cookies()
finally:
    try:
        browser.close()
    except:
        pass
