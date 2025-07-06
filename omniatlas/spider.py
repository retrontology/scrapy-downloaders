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

    def __init__(self, host:str, user:str, password:str, database:str, image_dir:str, port:int = 3306):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.image_dir = Path(self.image_dir)
        self.image_dir.mkdir(parents=False, exist_ok=True)


    @classmethod
    def from_crawler(cls, crawler: Crawler):
        return cls(
            host=crawler.settings.get("MYSQL_HOST"),
            port=crawler.settings.get("MYSQL_PORT", 3306),
            user=crawler.settings.get("MYSQL_USER"),
            password=crawler.settings.get("MYSQL_PASS"),
            database=crawler.settings.get("MYSQL_DB"),
            image_dir=crawler.settings.get("IMAGE_DIR")
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
                    url VARCHAR(255) NOT NULL,
                    path VARCHAR(255) NOT NULL,
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
        
        image = adapter.get('image')
        if not image:
            raise DropItem("Missing image")
        
        image_id = uuid4().hex

        region = adapter.get('region')
        region_slug = slugify(region)
        image_region_path = Path(self.image_dir) / region_slug
        image_region_path.mkdir(parents=False, exist_ok=True)

        image_name = image.split("/")[-1]
        image_path = image_region_path / image_name

        with open(image_path, 'wb') as f:
            f.write(requests.get(image).content)
        
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
                    url,
                    path
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            (
                image_id,
                region,
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
