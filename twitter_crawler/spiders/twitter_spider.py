import scrapy
from scrapy import http
from scrapy.selector import Selector
from datetime import datetime
import json

from twitter_crawler.items import TwitterCrawlerItem

class TwitterSpider(scrapy.Spider):
	
	name = "twitter"

	start_urls = ['https://twitter.com/i/search/timeline?f=tweets&q=hello&src=typed']

	def __init__(self):
		self.currentIteration = 0
		self.totalIterations = 5
		self.min_tweet = None

	def parse(self, response):

		data = json.loads(response.body.decode("utf-8"))
		response = Selector(text=data['items_html'])
		# sels = response.xpath('.//div[@class="stream"]/ol[contains(@class, "stream-items")]/li[contains(@class, "stream-item")]')
		sels = response.xpath('//li[@data-item-type="tweet"]/div')
		len_sels = len(sels)
		for i, sel in enumerate(sels):

			# 我们所搜索的词汇也会加粗，故断开
			item = TwitterCrawlerItem()
			
			# core parts of tweet
			item['tweetId'] = sel.xpath('.//a[contains(@class, "js-permalink")]/@data-conversation-id').extract()[0]
			item['text'] = ''.join(sel.xpath('.//div[@class="js-tweet-text-container"]/p//text()').extract())
			unix_time = int(sel.xpath('.//span[contains(@class, "_timestamp")]/@data-time').extract()[0])
			item['time'] = datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')

			# interactions
			item['retweets'] = int(sel.css('span.ProfileTweet-action--retweet > span.ProfileTweet-actionCount')
				.xpath('@data-tweet-stat-count').extract()[0])
			item['likes'] = int(sel.css('span.ProfileTweet-action--favorite > span.ProfileTweet-actionCount')
				.xpath('@data-tweet-stat-count').extract()[0])
			item['replies'] = int(sel.css('span.ProfileTweet-action--reply > span.ProfileTweet-actionCount')
				.xpath('@data-tweet-stat-count').extract()[0])

			# multimedias
			urls = []
			url_nodes = sel.css('a.twitter-timeline-link:not(.u-hidden)')
			for url_node in url_nodes:
				urls.append(url_node.xpath('@data-expanded-url').extract()[0])

			# 必须要加点，才会取当前元素
			hashtags = [''.join(hashtag_node.xpath('.//text()').extract()) for hashtag_node in sel.xpath('.//a[contains(@class, "twitter-hashtag")]')]
			
			photos = []
			photo_nodes = sel.xpath('.//div[contains(@class, "AdaptiveMedia-photoContainer")]')
			for photo_node in photo_nodes:
				# Selectorlist 返回的永远是list型数据
				photos.append(photo_node.xpath("@data-image-url").extract()[0])

			videos = []
			video_node_styles = sel.xpath('.//div[contains(@class, "PlayableMedia-player")]/@style').extract()
			
			if len(video_node_styles) > 0 :
				for style in video_node_styles:
					if style.startswith('background'):
						tmp = style.split('/')[-1]
						video_id = tmp[:tmp.index('.jpg')]
						videos.append({'id': video_id})

			entries = {
				'hashtags': hashtags, 'photos': photos,
				'urls': urls, 'videos': videos
			}

			item['entries'] = entries

			if self.min_tweet is None:
				self.min_tweet = item

			yield(item)

			
		self.currentIteration += 1
		if(self.currentIteration <= self.totalIterations):
			max_tweet = item
			# 相同的urlscrapy会自动停止
			# next_url = "https://twitter.com/search?f=tweets&q=jiechu li&src=typd" + "&time=" + str(self.currentIteration)
			if self.min_tweet['tweetId'] is not max_tweet['tweetId']:
				if "min_position" in data.keys():
					max_position = data['min_position']
				else:
					max_position = "TWEET-%s-%s" % (max_tweet['tweetId'], self.min_tweet['tweetId'])

				print("Search hints9 %d" %self.currentIteration)
				# data_min_position = response.xpath('.//div[@id="timeline"]//div[contains(@class, "stream-container")]/@data-min-position').extract()[0]
				# max_position = data_min_position.split('-')[-1]
				next_url = "https://twitter.com/i/search/timeline?f=tweets&q=hello&src=typd&max_position=" + max_position+ "&reset_error_state=false"
				yield http.Request(next_url, callback=self.parse)