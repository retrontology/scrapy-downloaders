from scrapy import Spider
from scrapy.crawler import Crawler
from scrapy.item import Item, Field
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, Join, Compose
from datetime import datetime
from itemadapter import ItemAdapter
import mysql.connector
import requests


class AtlasFrameDBPipeline:


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
            port=crawler.settings.get("MYSQL_PORT"),
            user=crawler.settings.get("MYSQL_USER"),
            password=crawler.settings.get("MYSQL_PASS"),
            database=crawler.settings.get("MYSQL_DB"),
        )


    def create_table(self):
        cursor = self.connection.cursor()
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS atlas_frame (
                    id INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
                    region VARCHAR(255) NOT NULL,
                    date DATE NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    description TEXT NOT NULL,
                    url VARCHAR(255) NOT NULL,
                    image BLOB NOT NULL
                )
            """
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


    def close_spider(self, spider):
        self.connection.close()


    def process_item(self, item, spider):
        cursor = self.connection.cursor()
        adapter = ItemAdapter(item)
        cursor.execute(
            """
                INSERT INTO atlas_frame (
                    region,
                    date,
                    title,
                    description,
                    url,
                    image
                )
                VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                adapter.get("region"),
                adapter.get("date"),
                adapter.get("title"),
                adapter.get("description"),
                adapter.get("url"),
                adapter.get("image")

            )
        )
        self.connection.commit()
        cursor.close()


class AtlasRegionFrame(Item):
    region = Field()
    date = Field(serializer=str)
    title = Field()
    image = Field(serializer=str)
    description = Field()
    url = Field()


class AtlasRegionFrameLoader(ItemLoader):
    default_output_processor = TakeFirst()
    description_in = Join('')
    date_in = Compose(
        lambda x: x[0],
        lambda x: x.strip(),
        lambda x: datetime.strptime(x, '%d %B %Y'),
        lambda x: x.date()
    )
    image_in = Compose(
        lambda x: x[0],
        lambda x: x.strip(),
        lambda x: requests.get(x).content
    )


class AtlasSpider(Spider):
    name = "omniatlas"
    start_urls = [
        "https://omniatlas.com/maps/arctic/18140114/",
        #"https://omniatlas.com/maps/north-america/14270907/",
        #"https://omniatlas.com/maps/south-america/18641213/",
        #"https://omniatlas.com/maps/europe/61231/",
        #"https://omniatlas.com/maps/northern-africa/200219/",
        #"https://omniatlas.com/maps/sub-saharan-africa/18961026/",
        #"https://omniatlas.com/maps/northern-eurasia/19040207/",
        #"https://omniatlas.com/maps/east-asia/19481106/",
        #"https://omniatlas.com/maps/southern-asia/19020115/",
        #"https://omniatlas.com/maps/asia-pacific/18940609/",
        #"https://omniatlas.com/maps/australasia/17880126/"
    ]


    @staticmethod
    def parse_frame(response) -> AtlasRegionFrame:
        loader = AtlasRegionFrameLoader(item=AtlasRegionFrame(), response=response)
        loader.add_xpath("region", '/html/body/main/div[1]/div/div[2]/div/section[1]/div[1]/div[3]/h3/a/text()')
        loader.add_xpath("date", '/html/body/main/div[1]/div/div[2]/div/section[1]/div[1]/div[1]/h3/a/text()')
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
