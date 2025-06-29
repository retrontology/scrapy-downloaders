from scrapy.crawler import CrawlerProcess
import logging
from pathlib import Path
from spider import AtlasSpider
from dotenv import load_dotenv
import os


def main():
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    load_dotenv()
    process = CrawlerProcess(
        settings={
            "ITEM_PIPELINES": {
                "spider.AtlasFrameDBPipeline": 300,
            },
            "MYSQL_HOST": os.getenv("MYSQL_HOST"),
            "MYSQL_PORT": os.getenv("MYSQL_PORT", 3306),
            "MYSQL_USER": os.getenv("MYSQL_USER"),
            "MYSQL_PASS": os.getenv("MYSQL_PASS"),
            "MYSQL_DB": os.getenv("MYSQL_DB"),
        }
    )
    process.crawl(AtlasSpider)
    process.start()
    process.join()


if __name__ == '__main__':
    main()
