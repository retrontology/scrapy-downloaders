import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.exporters import JsonItemExporter
import os
import sys
import string
import urllib
import re
import socket
import json

items = []
dlservers = ['192.168.1.1', '192.168.1.2', '192.168.1.3', '192.168.1.4', '192.168.1.5', '192.168.1.6', '192.168.1.7']
port = 42069

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

def sendToServer(host, msg, retry=3):
    tries = 0
    while tries < retry:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            #print("Sending to " + host + " try #" + str(tries) + ": " + msg)
            s.connect((host, port))
            s.send(msg.encode())
            data = s.recv(1024).decode()
            if data == 'OK':
                break
            else:
                tries += 1
                continue
        except:
            print(sys.exc_info()[0])
            tries += 1
            continue

def main():
    process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
    'ITEM_PIPELINES': { '__main__.ItemCollectorPipeline': 100 }})
    process.crawl(archiveDLSpider)
    process.start()
    print("Distributing files to be downloaded now...")
    index = 0
    for item in items:
        item['directory'] = os.path.join(os.path.dirname(os.path.realpath(__file__)), item['directory'])
        if not os.path.exists(os.path.join(item['directory'], item['name'])):
            msg = json.dumps({'name': item['name'], 'directory': item['directory'], 'url': item['url']})
            sendToServer(dlservers[index], msg)
            index += 1
            if index >= len(dlservers): index = 0
    for server in dlservers:
        sendToServer(server, 'END')

if __name__ == '__main__':
    main()
