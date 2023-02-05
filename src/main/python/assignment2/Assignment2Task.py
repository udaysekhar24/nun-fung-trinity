import os
import logging
import pandas as pd
from bs4 import BeautifulSoup
import requests
import calendar
from datetime import datetime

# Logging setup
logger = logging.getLogger(__name__)

MAX_NUM_PAGES = 3
SIZE = 40
PARENT_URL = "https://www.michaelkors.co.uk/handbags/view-all-handbags/_/N-10qbalf"
TARGET_DIR = r'C:\Users\UDAY\IdeaProjects\nunfung-trinity-assignment\target\\assignment2\\'


class Product(object):

    def __init__(self):
        self.product_brand = ''
        self.product_name = ''
        self.price = ''
        self.timestamp_ms = calendar.timegm(datetime.utcnow().utctimetuple())


class Assignment2Task:
    @staticmethod
    def fetch_data(start):
        pagination_url_part = "&No={start}&Nrpp={size}"
        page_url = PARENT_URL + pagination_url_part.format(start=start, size=SIZE)
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0", }
        response = requests.get(page_url, headers=headers, timeout=120, allow_redirects=False)
        return response.content

    @staticmethod
    def parse_html(stream):
        try:
            soup = BeautifulSoup(stream, features="html.parser")
            product_lst = list()
            for panel in soup.find_all('ul', class_='description-panel text-left'):
                new_product = Product()
                new_product.product_brand = panel.find_next('li', class_='product-brand-container').a.get_text()
                new_product.product_name = panel.find_next('li', class_='product-name-container').a.get('title')
                new_product.price = panel.find_next('span', class_='ada-link productAmount').get_text()

                product_lst.append(new_product.__dict__)
            return product_lst
        except Exception as e:
            raise

    def run(self):
        logger.info("^^^^~~~~~~^^^^ Here are the dragons ^^^^~~~~~~^^^^")
        logger.info("Web scraping " + str(MAX_NUM_PAGES) + " with page size " + str(SIZE) + " from " + PARENT_URL)
        start = 0
        for counter in range(1, MAX_NUM_PAGES+1):
            try:
                logger.info("For page stat " + str(start))
                start = (start * counter) + SIZE

                logger.info(">>>> Fetching content")
                html_body = self.fetch_data(start=start)

                logger.info(">>>> Scraping")
                product_lst = self.parse_html(html_body)
                product_df = pd.DataFrame(product_lst)

                logger.info(">>>> Storing")
                path = os.path.join(TARGET_DIR, str(counter) + r'output.csv')
                product_df.to_csv(path, index=None)
            except Exception as e:
                logger.warning("Failed for page start " + str(start))
        logger.info("Task completed.")


if __name__ == "__main__":
    a2t = Assignment2Task
    a2t.run()

