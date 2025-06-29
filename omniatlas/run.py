from scrapy.crawler import CrawlerProcess
import logging
from spider import AtlasSpider
from dotenv import load_dotenv
import os
import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="Scrape OmniAtlas into a sqlite database")
    parser.add_argument(
        "-m", "--mysql",
        action="store_true",
        default=False,
        help="A toggle to use the MySQL pipeline instead of sqlite. Note: you will need to set the appropriate environment variables"
    )
    parser.add_argument(
        "-d", "--dbfile",
        default='omniatlas.sqlite',
        required=False,
        help='The path to the sqlite file you want to store the data in. Default: omniatlas.sqlite'
    )
    args = parser.parse_args()
    return args


def main():
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    args = parse_args()
    if args.mysql:
        print('Using MySQL pipeline')
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
        }
    else:
        print('Using sqlite pipeline')
        settings = {
            "ITEM_PIPELINES": {
                "spider.AtlasFrameSQLitePipeline": 300,
            },
            "SQLITE_DB_PATH": args.dbfile,
        }
    process = CrawlerProcess(
        settings=settings
    )
    process.crawl(AtlasSpider)
    process.start()
    process.join()


if __name__ == '__main__':
    main()
