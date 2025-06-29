from scrapy.crawler import CrawlerProcess
import logging
from pathlib import Path
from spider import AtlasSpider


def main():
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    json_export = Path('output.json')
    if json_export.exists():
        json_export.unlink()
    process = CrawlerProcess(
        settings={
            "FEEDS": {
                json_export: {"format": "json"},
            },
            "MYSQL_HOST": "",
            "MYSQL_PORT": "",
            "MYSQL_USER": "",
            "MYSQL_PASS": "",
            "MYSQL_DB": "",
        }
    )
    process.crawl(AtlasSpider, start_urls=["https://omniatlas.com/maps/south-america/18641213/"])
    process.start()
    process.join()


if __name__ == '__main__':
    main()
