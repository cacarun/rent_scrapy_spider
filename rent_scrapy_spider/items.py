# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class RentScrapySpiderItem(scrapy.Item):
    # define the fields for your item here like:
    title = scrapy.Field()
    user_name = scrapy.Field()
    last_updated_time = scrapy.Field()
    url = scrapy.Field()
    craw_time = scrapy.Field()

    content = scrapy.Field()
    city = scrapy.Field()

    area = scrapy.Field()
    address = scrapy.Field()
    contact = scrapy.Field()

    pass
