#!/usr/bin/env python3

import scrapy
from scrapy.crawler import CrawlerProcess
import json
import os
import requests
from multiprocessing.pool import Pool

DL_URLS = [
    'https://synthesiamaniac.com/downloads/animal-crossing/',
    'https://synthesiamaniac.com/downloads/cave-story/',
    'https://synthesiamaniac.com/downloads/donkey-kong/',
    'https://synthesiamaniac.com/downloads/final-fantasy/',
    'https://synthesiamaniac.com/downloads/legend-of-zelda/',
    'https://synthesiamaniac.com/downloads/star-fox/',
    'https://synthesiamaniac.com/downloads/medleys/',
    'https://synthesiamaniac.com/downloads/more-games/',
    'https://synthesiamaniac.com/downloads/miscellaneous/'
]
MIDI_URL_JSON = os.path.join(os.getcwd(), 'midi_urls.json')
DL_DIR = os.path.join(os.getcwd(), 'midis')
DL_INSTANCES = 4

class synthesiamaniacDL(scrapy.Spider):

    name = "synthesiamanicDL"

    def start_requests(self):
        urls = DL_URLS
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        midi_sections = response.xpath('//div[contains(@class, "czr-wp-the-content")]/div[contains(@class, "wp-block-pb-accordion-item")]')
        for section in midi_sections:
            section_name = section.xpath('h4/text()')[0].get()
            midi_urls = section.xpath('div[contains(@class, "c-accordion__content")]/table/tr/td[contains(@class, "midi-download")]/a/@href').getall()
            yield {
                'name': section_name,
                'urls': midi_urls
            }

def fetch_midi_urls(export_name=MIDI_URL_JSON):
    if os.path.isfile(export_name):
        os.remove(export_name)
    process = CrawlerProcess(
        settings={
            "FEEDS": {
                export_name: {"format": "json"},
            },
        }
    )
    process.crawl(synthesiamaniacDL)
    process.start()
    process.join()

def download_midis(midi_url_json=MIDI_URL_JSON, dl_dir=DL_DIR, dl_instances=DL_INSTANCES):
    print('Starting to download the MIDIs now...')
    downloader_pool = Pool(dl_instances)
    with open(midi_url_json, 'rb') as infile:
        midi_url_map = json.load(infile)
    for section in midi_url_map:
        section_dir = os.path.join(dl_dir, section['name'])
        os.makedirs(section_dir, exist_ok=True)
        for url in section['urls']:
            midi_path = os.path.join(section_dir, url[url.rfind('/')+1:])
            if not os.path.isfile(midi_path):
                downloader_pool.apply(download_file, (url, midi_path))
    downloader_pool.close()
    downloader_pool.join()
    print('All MIDIs have been downloaded!')

def download_file(url, path, retry=3):
    print("Downloading: " + url + " to " + path)
    attempts = 0
    while attempts < retry:
        try:
            r = requests.get(url, allow_redirects=True, timeout=5, stream=True)
            if r.status_code == 200:
                size = int(r.headers.get('Content-Length'))
                with open(path+".part", 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        f.write(chunk)
                os.rename(path+".part", path)
                print("Downloaded: " + path)
                break
            else: attempts += 1
        except Exception as e:
            attempts += 1
            print(e)
        if attempts == retry - 1:
            print("Could not download: " + url)


if __name__ == '__main__':
    fetch_midi_urls()
    download_midis()
