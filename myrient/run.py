#!/usr/bin/env python3

import scrapy
from scrapy.crawler import CrawlerProcess
import json
from pathlib import Path
import requests
from multiprocessing.pool import Pool

ROOT_URL = 'https://myrient.erista.me/files/Internet%20Archive/chadmaster/chd_psx/CHD-PSX-USA/'
DEFAULT_JSON = 'psx.json'

DL_DIR = '/mnt/media/Games/psx'
DL_INSTANCES = 4

def main():
    crawl()
    download_images()

class myrientScraper(scrapy.Spider):

    name = "myrientScraper"

    def start_requests(self):
        yield scrapy.Request(url=ROOT_URL, callback=self.parse)

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

def crawl(json_export=DEFAULT_JSON):
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
    process.crawl(myrientScraper)
    process.start()
    process.join()


def download_images(url_json=DEFAULT_JSON, dl_dir=DL_DIR, dl_instances=DL_INSTANCES):

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
