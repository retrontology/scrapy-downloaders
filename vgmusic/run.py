#!/usr/bin/env python3

import scrapy
from scrapy.crawler import CrawlerProcess
import json
import os
import requests
from multiprocessing.pool import Pool
from math import floor
from functools import partial

SITEMAP = 'http://www.vgmusic.com/information/sitemap.php'
MIDI_URL_JSON = os.path.join(os.getcwd(), 'midi_urls.json')
DL_DIR = os.path.join(os.getcwd(), 'midis')
DL_INSTANCES = 4

def main():
    fetch_midi_urls()
    #download_midis()

class vgmusicDL(scrapy.Spider):

    name = "vgmusicDL"

    def start_requests(self):
        yield scrapy.Request(url=SITEMAP, callback=self.parse_sitemap)

    def parse_sitemap(self, response):
        tables = response.xpath('//table')
        for table in tables:
            title = table.xpath('tr/td/span/text()').get()
            if title == 'Music':
                self.parse_music_table(table)
                break

    def parse_music_table(self, table):
        rows = table.xpath('tr')
        for i in range(1, floor(len(rows))/2):
            genre = rows[i*2-1].xpath('td/span/text()').get()
            print(genre)
            for list in rows[i*2].xpath('td/ul/li'):
                manufacturer = list.xpath('text()').get()[:-1]
                for link in list.xpath('a'):
                    system = link.xpath('text()').get()
                    url = link.xpath('@href').get()
                    callback = partial(self.parse_system, genre, manufacturer, system)
                    yield scrapy.Request(url=url, callback=callback)

    def parse_system(self, genre, manufacturer, system, response):
        data = {
            'genre': genre,
            'manufacturer': manufacturer,
            'system': system,
            'games': {}
        }
        rows = response.xpath('//table/tr')
        game = ''
        i = 0
        while i < len(rows):
            if rows[i].attrib['class'] == 'header':
                game = rows[i].xpath('td/a/text()').get()
                data['games'][game] = {}
            elif rows[i].xpath('td/a').get():
                title = rows[i].xpath('td/a/text()')
                link = rows[i].xpath('td/a/@href')
                data['games'][game][title] = link
            i+=1
        yield data

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
    process.crawl(vgmusicDL)
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
    main()
