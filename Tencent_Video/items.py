# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class TencentVideoItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class VideoListItem(scrapy.Item):
    info = scrapy.Field()

class VidItem(scrapy.Item):
    info = scrapy.Field()

class PlayInfoItem(scrapy.Item):
    info = scrapy.Field()