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
import sqlite3


class AtlasFramePipeline:

    value_character: str
    connection: sqlite3.Connection|mysql.connector.MySQLConnection


    @classmethod
    def from_crawler(cls, crawler: Crawler):
        pass


    def create_table(self):
        cursor = self.connection.cursor()
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS atlas_frame_image (
                    id VARCHAR(36) PRIMARY KEY NOT NULL,
                    image MEDIUMBLOB NOT NULL,
                );
            """
        )
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS atlas_frame (
                    id VARCHAR(36) PRIMARY KEY NOT NULL,
                    region VARCHAR(255) NOT NULL,
                    date DATE NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    description TEXT NOT NULL,
                    url VARCHAR(255) NOT NULL,
                    image VARCHAR(36) NOT NULL,
                    FOREIGN KEY (image) REFERENCES atlas_frame_image(id) ON DELETE CASCADE
                );
            """
        )
        cursor.close()
        self.connection.commit()


    def open_spider(self, spider):
        pass


    def close_spider(self, spider):
        self.connection.close()


    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        image = adapter.get('image')
        if not image:
            raise DropItem("Missing image")
        image_data = requests.get(image).content
        image_id = uuid4().hex
        frame_date = adapter.get('date')
        frame_date = f"{frame_date.year:04d}-{frame_date.month:02d}-{frame_date.day:02d}"
        cursor = self.connection.cursor()
        cursor.execute(
            f"""
                INSERT INTO atlas_frame_image (
                    id,
                    image
                )
                VALUES (
                    {self.value_character},
                    {self.value_character}
                );
            """,
            (image_id, image_data)
        )
        cursor.execute(
            f"""
                INSERT INTO atlas_frame (
                    id,
                    region,
                    date,
                    title,
                    description,
                    url,
                    image
                )
                VALUES (
                    {self.value_character},
                    {self.value_character},
                    {self.value_character},
                    {self.value_character},
                    {self.value_character},
                    {self.value_character},
                    {self.value_character}
                );
            """,
            (
                image_id,
                adapter.get('region'),
                frame_date,
                adapter.get('title'),
                adapter.get('description'),
                adapter.get('url'),
                image_id
            )
        )
        self.connection.commit()
        cursor.close()
        return item


class AtlasFrameSQLitePipeline(AtlasFramePipeline):

    value_character = "?"

    def __init__(self, path:str):
        self.path = Path(path)


    @classmethod
    def from_crawler(cls, crawler: Crawler):
        return cls(
            path=crawler.settings.get("SQLITE_DB_PATH"),
        )


    def open_spider(self, spider):
        self.connection = sqlite3.connect(self.path)
        self.create_table()


class AtlasFrameMySQLPipeline(AtlasFramePipeline):

    value_character = "%s"

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


    def open_spider(self, spider):
        self.connection = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.create_table()


class AtlasRegionFrame(Item):
    region = Field()
    date = Field(serializer=str)
    title = Field()
    image = Field(serializer=str)
    description = Field()
    url = Field()


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
        #loader.add_xpath("date", '/html/body/main/div[1]/div/div[2]/div/section[1]/div[1]/div[1]/h3/a/text()')
        loader.add_xpath("title", '/html/body/main/div[1]/div/div[2]/div/section[2]/h2/a/text()')
        loader.add_xpath("image", '/html/body/main/div[1]/div/div[2]/div/section[2]/div[2]/div/div[2]/a[1]/@href')
        loader.add_xpath("description", '/html/body/main/div[1]/div/div[2]/div/section[2]/div[2]/p/text() | /html/body/main/div[1]/div/div[2]/div/section[2]/div[2]/p/a/text()')
        loader.add_value("url", response.url)
        return loader.load_item()


    def parse(self, response, **kwargs):
        yield self.parse_frame(response)
        next_page = response.css('a.btn:nth-child(4)').xpath('@href').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)
