# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class TwitterCrawlerItem(scrapy.Item):
    
    tweetId = scrapy.Field()
    publisherInfo = scrapy.Field()
    time = scrapy.Field()
    text = scrapy.Field()
    replies = scrapy.Field()
    retweets = scrapy.Field()
    likes = scrapy.Field()
    entries = scrapy.Field()
