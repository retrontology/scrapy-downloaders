from scrapy.crawler import CrawlerProcess
import logging
from spider import AtlasSpider
from dotenv import load_dotenv
import os
import argparse
from pathlib import Path


def main():
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    load_dotenv()
    settings = {
        "ITEM_PIPELINES": {
            "spider.AtlasFrameMySQLPipeline": 300,
        },
        "MYSQL_HOST": os.getenv("MYSQL_HOST"),
        "MYSQL_PORT": os.getenv("MYSQL_PORT", 3306),
        "MYSQL_USER": os.getenv("MYSQL_USER"),
        "MYSQL_PASS": os.getenv("MYSQL_PASS"),
        "MYSQL_DB": os.getenv("MYSQL_DB"),
        "IMAGE_DIR": os.getenv("IMAGE_DIR", 'images')
    }
    process = CrawlerProcess(
        settings=settings
    )
    process.crawl(AtlasSpider)
    process.start()
    process.join()


if __name__ == '__main__':
    main()
