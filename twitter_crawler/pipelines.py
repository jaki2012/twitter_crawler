# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem
import json
import codecs
import os

class TwitterCrawlerPipeline(object):
    
    def __init__(self):
        self.ids_collected = set()
        self.items_nums = 0
        self.first_item_saved = False
        self.file = codecs.open('twitters.json', 'w', encoding='utf-8')
        self.file.write('[' + '\n')

    def process_item(self, item, spider):
        if self.first_item_saved:
            self.file.write(',\n')
        else:
            self.first_item_saved = True
        self.items_nums += 1
        if item['tweetId'] in self.ids_collected:
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.ids_collected.add(item['tweetId'])
            line = json.dumps(dict(item), ensure_ascii=False)
            self.file.write(line)
            print("Uniques tweets/Total tweets: %d/%d" % (len(self.ids_collected), self.items_nums))
            return item

    # spider_closed() function is deprecated.
    def close_spider(self, spider):
        self.file.write('\n')
        self.file.write(']')
        self.file.close()