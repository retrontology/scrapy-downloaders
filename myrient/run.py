#!/usr/bin/env python3

import scrapy
from scrapy.crawler import CrawlerProcess
import json
from pathlib import Path
import requests
from multiprocessing.pool import Pool
from argparse import ArgumentParser
import logging

DEFAULT_JSON = 'images.json'
DL_DIR = 'download'
DL_INSTANCES = 4

def main():
    logging.getLogger('scrapy').setLevel(logging.WARNING)
    args = parse_args()
    crawl(
        url=args.url,
        json_export=args.json,
    )
    download_images(
        url_json=args.json,
        dl_dir=args.output,
        dl_instances=args.instances
    )

def parse_args():
    parser = ArgumentParser(
        prog='myrientCrawler',
        description='Crawls links from a single page on myrient.erista.me and subsequently downloads all of the images',
    )
    parser.add_argument(
        'url',
        help='The full url for the page on myrient.erista.me you want to parse'
    )
    parser.add_argument(
        '-j', '--json',
        default=DEFAULT_JSON,
        help=f'The name of the json file where the link mappings are stored. If omitted will default to {DEFAULT_JSON}'
    )
    parser.add_argument(
        '-o', '--output',
        default=DL_DIR,
        help=f'The directory where you wish to store the downloaded images. If omitted will default to {DL_DIR}'
    )
    parser.add_argument(
        '-i', '--instances',
        default=DL_INSTANCES,
        help=f'The number of instances you wish to use for the multiprocessing pool that will download the images. If omitted will default to {DL_INSTANCES}'
    )
    return parser.parse_args()

class myrientScraper(scrapy.Spider):

    name = "myrientScraper"

    def parse(self, response):
        links = response.xpath('//table[@id="list"]/tbody/tr/td[contains(@class, "link")]/a')
        for link in links:
            name = link.xpath('text()').get()
            url = link.xpath('@href').get()
            if url != '../':
                yield {
                    'name': name,
                    'url': response.urljoin(url)
                }

def crawl(url:str, json_export:str):
    json_export = Path(json_export)
    if json_export.exists():
        json_export.unlink()
    process = CrawlerProcess(
        settings={
            "FEEDS": {
                json_export: {"format": "json"},
            },
        }
    )
    process.crawl(myrientScraper, start_urls=[url])
    process.start()
    process.join()


def download_images(url_json:str, dl_dir:str, dl_instances:int):

    print('Starting to download the images now...')

    url_json = Path(url_json)
    dl_dir = Path(dl_dir)
    dl_dir.mkdir(exist_ok=True)

    with open(url_json, 'rb') as infile:
        url_dict = json.load(infile)

    pool = Pool(dl_instances)

    for image in url_dict:
        path = dl_dir.joinpath(image['name'])
        pool.apply_async(download_file, (image['url'], path))
                
    pool.close()
    pool.join()

    print('All images have been downloaded!')

def download_file(url: str, path: Path, retry:int=3):

    print("Downloading: " + url + " to " + path.__str__())

    attempts = 0
    while attempts < retry:

        try:
            response = requests.get(url, allow_redirects=True, timeout=5, stream=True)
            if response.status_code == 200:
                size = int(response.headers.get('Content-Length'))
                temp_path = path.parent.joinpath(path.stem + '.part')
                with open(temp_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=1024):
                        f.write(chunk)
                temp_path.rename(path)
                print("Downloaded: " + path.__str__())
                break
            else: attempts += 1

        except Exception as e:
            attempts += 1
            print(e)

        if attempts == retry - 1:
            print("Could not download: " + url)

if __name__ == '__main__':
    main()
