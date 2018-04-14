# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem

class TwitterCrawlerPipeline(object):
	
	def __init__(self):
		self.ids_collected = set()
		self.items_nums = 0

	def process_item(self, item, spider):
		self.items_nums += 1
		if item['tweetId'] in self.ids_collected:
			raise DropItem("Duplicate item found: %s" % item)
		else:
			self.ids_collected.add(item['tweetId'])
			print("Uniques tweets/Total tweets: %d/%d" % (len(self.ids_collected), self.items_nums))
			return item