from selenium import webdriver
import time


try:
    options = webdriver.FirefoxOptions()
    options.headless = True
    browser = webdriver.Firefox(options=options)

    extension_dir = "C:\\Users\\daank\\PycharmProjects\\SCM\\selenium\\extensions\\"
    extensions = ["jid1-KKzOGWgsW3Ao4Q@jetpack.xpi"]
    for extension in extensions:
        browser.install_addon(extension_dir + extension, temporary=True)

    browser.get("https://tweakers.net/")
    time.sleep(5)  # To allow the the accept button to be clicked
    print(browser.page_source)

    browser.delete_all_cookies()
finally:
    try:
        browser.close()
    except:
        pass
