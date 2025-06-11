import scrapy
from scrapy.http import Request
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


class SinaSpiderSpider(scrapy.Spider):
    name = 'sina_spider'


    def __init__(self):
        self.start_urls = ['https://news.sina.com.cn/china/']
        self.option = self._set_chrome_options()
        self.service = Service("/home/xwxsee/chromedriver-linux64/chromedriver")

    def _set_chrome_options(self) -> Options:
        """
        Sets chrome options for Selenium.
        Chrome options for headless browser is enabled.
        """
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument('--blink-setting=imagesEnable=false')
        return chrome_options

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse)

    def parse(self, response):
        driver = webdriver.Chrome(service=self.service, options=self.option)
        driver.set_page_load_timeout(30)
        driver.get(response.url)

        title = driver.find_elements('xpath', "//h2[@class='undefined']/a[@target='_blank']")
        time = driver.find_elements('xpath', "//h2[@class='undefined']/../div[@class='feed-card-a "
                                             "feed-card-clearfix']/div[@class='feed-card-time']")

        for i in range(len(title)):
            print(title[i].text)
            print(time[i].text)
