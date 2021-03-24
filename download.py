import scrapy
from scrapy.crawler import CrawlerProcess
import sys
import requests
import os
import string
import multiprocessing
import urllib
import re
import time
import progressbar

class fileItem(scrapy.Item):
    name = scrapy.Field()
    url = scrapy.Field()
    directory = scrapy.Field()

class ItemCollectorPipeline(object):
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        items.append(item)

class archiveDLSpider(scrapy.Spider):
    name = 'archiveDL'
    allowed_domains = ['archive.org']
    start_urls = ['https://archive.org/download/RedumpSegaDreamcast20160613', 'https://archive.org/download/redump.psp', 'https://archive.org/download/redump.psp.p2', 'https://archive.org/download/GameboyClassicRomCollectionByGhostware', 'https://archive.org/download/GameboyColorRomCollectionByGhostware', 'https://archive.org/download/GameboyAdvanceRomCollectionByGhostware']
    reg = re.compile('.+\.zip')
    def parse(self,response):
        files = response.xpath('//table[@class="directory-listing-table"]/tbody/tr/td/a')
        if files:
            for f in files:
                name = f.xpath('text()').extract_first()
                if self.reg.match(name):
                    item = fileItem()
                    item['name'] = name
                    item['url'] = response.url + "/" + f.xpath('@href').extract_first()
                    item['directory'] = response.url.rsplit('/')[-1]
                    yield item

def download(url, path, rename=False, attempts=3):
    if os.path.exists(path) and not rename:
        print(path + " already exists! Skipping!")
    else:
        if os.path.exists(path) and rename:
            filename, ext = os.path.basename(path).rsplit('.', maxsplit=1)
            newPath = path
            count = 1
            while os.path.exists(newPath):
                newPath = os.path.join(os.path.dirname(path), filename + "[" + count + "]" + "." + ext)
                count = count + 1
            path = newPath
        try:
            print("Downloading: " + url + " to " + path)
            for i in range(attempts):
                r = requests.get(url, allow_redirects=True)
                if r.status_code == 200:
                    open(path, 'wb').write(r.content)
                    print("Downloaded: " + path)
                    break
                if i == attempts - 1:
                    print("Could not download: " + url)
        except KeyboardInterrupt:
            raise
        except:
            pass

def main():
    try:
        process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
        'ITEM_PIPELINES': { '__main__.ItemCollectorPipeline': 100 }})
        process.crawl(archiveDLSpider)
        process.start()
        print("Downloading files now...")
        pool = multiprocessing.get_context("spawn").Pool(4)
        processes = []
        for item in items:
            dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), item['directory'])
            os.makedirs(dir, exist_ok=True)
            processes.append(pool.apply_async(download, args = (item['url'], os.path.join(dir, item['name']))))
        pool.close()
        completed = 0
        progress = progressbar.bar.ProgressBar(max_value=len(processes)).start()
        while completed < len(processes):
            completed = 0
            for p in processes:
                if p.ready(): completed += 1
            progress.update(completed)
            time.sleep(1)
        progress.finish()
        pool.join()
        print("Finished!")
    except KeyboardInterrupt:
        pool.terminate()

if __name__ == '__main__':
    items = []
    main()
