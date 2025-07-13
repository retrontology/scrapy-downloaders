from scrapy import Spider
from scrapy.crawler import Crawler
from scrapy.item import Item, Field
from scrapy.loader import ItemLoader
from scrapy.exceptions import DropItem
from itemadapter.adapter import ItemAdapter
from itemloaders.processors import TakeFirst, Join, Compose
import mysql.connector
import requests
from urllib.parse import urlparse
from datetime import date
from uuid import uuid4
from pathlib import Path
from util import slugify


class AtlasFramePipeline:

    def __init__(self, host:str, user:str, password:str, database:str, port:int = 3306):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database


    @classmethod
    def from_crawler(cls, crawler: Crawler):
        return cls(
            host=crawler.settings.get("MYSQL_HOST"),
            port=crawler.settings.get("MYSQL_PORT", 3306),
            user=crawler.settings.get("MYSQL_USER"),
            password=crawler.settings.get("MYSQL_PASS"),
            database=crawler.settings.get("MYSQL_DB"),
        )


    def create_table(self):
        cursor = self.connection.cursor()
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS atlas_frame (
                    id VARCHAR(36) PRIMARY KEY NOT NULL,
                    region VARCHAR(255) NOT NULL,
                    date DATE NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    description TEXT NOT NULL,
                    url VARCHAR(255) NOT NULL
                );
            """
        )
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS atlas_data (
                    id VARCHAR(36) PRIMARY KEY NOT NULL,
                    data MEDIUMTEXT NOT NULL,
                    FOREIGN KEY (id) REFERENCES atlas_frame(id) ON DELETE CASCADE
                );
            """
        )
        cursor.close()
        self.connection.commit()


    def open_spider(self, spider):
        self.connection = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.create_table()


    def close_spider(self, spider):
        self.connection.close()


    def process_item(self, item, spider):

        adapter = ItemAdapter(item)
        
        id = uuid4().hex
        
        frame_date = adapter.get('date')
        frame_date = f"{frame_date.year:04d}-{frame_date.month:02d}-{frame_date.day:02d}"

        cursor = self.connection.cursor()
        cursor.execute(
            f"""
                INSERT INTO atlas_frame (
                    id,
                    region,
                    date,
                    title,
                    description,
                    url
                )
                VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (
                id,
                adapter.get('region'),
                frame_date,
                adapter.get('title'),
                adapter.get('description'),
                adapter.get('url')
            )
        )
        cursor.execute(
            f"""
                INSERT INTO atlas_data (
                    id,
                    data
                )
                VALUES (%s, %s);
            """,
            (
                id,
                adapter.get('data')
            )
        )
        self.connection.commit()
        cursor.close()
        return item


class AtlasRegionFrame(Item):
    region = Field()
    date = Field(serializer=str)
    title = Field()
    description = Field()
    url = Field()
    data = Field()


class AtlasRegionFrameLoader(ItemLoader):


    @staticmethod
    def parse_date(value):
        value = value[0]
        year, month, day = value[:-4], value[-4:-2], value[-2:]
        return date(int(year), int(month), int(day))


    default_output_processor = TakeFirst()
    description_in = Join('')
    date_in = Compose(parse_date)


class AtlasSpider(Spider):
    name = "omniatlas"
    start_urls = [
        "https://omniatlas.com/maps/arctic/18140114/",
        "https://omniatlas.com/maps/north-america/14270907/",
        "https://omniatlas.com/maps/south-america/18641213/",
        "https://omniatlas.com/maps/europe/61231/",
        "https://omniatlas.com/maps/northern-africa/200219/",
        "https://omniatlas.com/maps/sub-saharan-africa/18961026/",
        "https://omniatlas.com/maps/northern-eurasia/19040207/",
        "https://omniatlas.com/maps/east-asia/19481106/",
        "https://omniatlas.com/maps/southern-asia/19020115/",
        "https://omniatlas.com/maps/asia-pacific/18940609/",
        "https://omniatlas.com/maps/australasia/17880126/",
        "https://omniatlas.com/maps/eastern-mediterranean/61123/"
    ]


    @staticmethod
    def parse_frame(response) -> AtlasRegionFrame:
        loader = AtlasRegionFrameLoader(item=AtlasRegionFrame(), response=response)
        loader.add_xpath("region", '/html/body/main/div[1]/div/div[2]/div/section[1]/div[1]/div[3]/h3/a/text()')
        loader.add_value("date", urlparse(response.url).path.rsplit('/', 2)[1])
        loader.add_xpath("title", '/html/body/main/div[1]/div/div[2]/div/section[2]/h2/a/text()')
        loader.add_xpath("description", '/html/body/main/div[1]/div/div[2]/div/section[2]/div[2]/p/text() | /html/body/main/div[1]/div/div[2]/div/section[2]/div[2]/p/a/text()')
        loader.add_value("url", response.url)
        loader.add_xpath("data", '//*[@id="mainMap"]')
        return loader.load_item()


    def parse(self, response, **kwargs):
        yield self.parse_frame(response)
        next_page = response.css('a.btn:nth-child(4)').xpath('@href').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)
