import scrapy

from twitter_crawler.items import TwitterCrawlerItem

class TwitterSpider(scrapy.Spider):
	name = "twitter"

	start_urls = ['https://twitter.com/search?f=tweets&q=hello&src=typd']

	def parse(self, response):
		for sel in response.xpath('.//div[@class="stream"]/ol[contains(@class, "stream-items")]/li[contains(@class, "stream-item")]'):
			# 我们所搜索的词汇也会加粗，故断开
			item = TwitterCrawlerItem()
			
			# core parts of tweet
			item['tweetId'] = sel.xpath('.//a[contains(@class, "js-permalink")]/@data-conversation-id').extract()
			item['text'] = ''.join(sel.xpath('.//div[@class="js-tweet-text-container"]/p//text()').extract())
			item['time'] = sel.xpath('.//span[contains(@class, "_timestamp")]/@data-time').extract()

			# interactions
			item['retweets'] = int(sel.css('span.ProfileTweet-action--retweet > span.ProfileTweet-actionCount')
				.xpath('@data-tweet-stat-count').extract()[0])
			item['likes'] = int(sel.css('span.ProfileTweet-action--favorite > span.ProfileTweet-actionCount')
				.xpath('@data-tweet-stat-count').extract()[0])
			item['replies'] = int(sel.css('span.ProfileTweet-action--reply > span.ProfileTweet-actionCount')
				.xpath('@data-tweet-stat-count').extract()[0])

			# multimedias
			urls = [url_node.xpath('@data-expanded-url').extract for url_node in sel.css('a.twitter-timeline-link:not(.u-hidden)')]
			hashtags = [''.join(hashtag_node.xpath('//text()')) for hashtag_node in sel.xpath('//a[contains(@class, "twitter-hashtag")]')]

			print(hashtags)
			print(urls)
