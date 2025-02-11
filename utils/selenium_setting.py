import selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def setup_driver():
    """設定 Selenium 瀏覽器參數。"""
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("disable-extensions")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("window-size=1080,720")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--allow-insecure-localhost")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--enable-unsafe-swiftshader")
    driver = webdriver.Remote(
        command_executor="http://host.docker.internal:14444/wd/hub",
        options=chrome_options,
    )
    return driver