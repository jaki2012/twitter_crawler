import scrapy
from scrapy import http
from scrapy.selector import Selector
from datetime import datetime
import json
import os

from twitter_crawler.items import TwitterCrawlerItem

class TwitterSpider(scrapy.Spider):
    
    name = "twitter"

    def __init__(self, query=None, *args ,**kwargs):

        super(TwitterSpider, self).__init__(*args, **kwargs)

        self.url_base = 'https://twitter.com/i/search/timeline?f=tweets&src=typd&q={}'
        self.start_urls = []
        self.queries = []
        self.temp_query = None

        if kwargs.get('file') is not None:
            filename = os.path.join(os.getcwd(), kwargs.get('file'))
            with open(filename, 'r') as file:
                keywords = file.readline().strip().split(',')
                if len(keywords) == 0:
                    print("No query word specified, please check your command.")
                else:
                    self.start_urls = []
                    for keyword in keywords:
                        self.start_urls.append(self.construct_url(keyword, **kwargs))
                        self.queries.append(self.temp_query)
        else:
            if query is None:
                print("No query word specified, please check your command.")
            else:
                self.start_urls = [self.construct_url(query, **kwargs)]
                self.queries.append(self.temp_query)

        self.currentIteration = 0
        self.totalIterations = 2500
        self.min_tweet = None

        # To verify if each start_url corresponds to its original query
        for i, start_url in enumerate(self.start_urls):
            print("%s with %s" % (self.queries[i], start_url))

    def start_requests(self):
        for i, url in enumerate(self.start_urls):
            yield http.Request(url, dont_filter=True, meta={"query": self.queries[i]})

    def construct_url(self, query, **kwargs):
        constructed_url = self.url_base
        if kwargs.get("since") is not None:
            query += (" since:" + kwargs.get("since"))
        constructed_url  = self.url_base.format(query)
        if kwargs.get('language') is not None:
                constructed_url  += ("&l=" + kwargs.get("language"))
        self.temp_query = query
        return constructed_url

    def parse(self, response):

        data = json.loads(response.body.decode("utf-8"))
        response_selec = Selector(text=data['items_html'])
        # sels = response.xpath('.//div[@class="stream"]/ol[contains(@class, "stream-items")]/li[contains(@class, "stream-item")]')
        sels = response_selec.xpath('//li[@data-item-type="tweet"]/div')
        len_sels = len(sels)
        for i, sel in enumerate(sels):

            item = TwitterCrawlerItem()
            
            # core parts of tweet
            item['tweetId'] = sel.xpath('.//a[contains(@class, "js-permalink")]/@data-conversation-id').extract()[0]
            item['text'] = ''.join(sel.xpath('.//div[@class="js-tweet-text-container"]/p//text()').extract())
            unix_time = int(sel.xpath('.//span[contains(@class, "_timestamp")]/@data-time').extract()[0])
            item['time'] = datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')

            # information of the publisher
            publisherInfo = {}
            username = sel.xpath('.//@data-name').extract()[0]
            screenName = "@" + sel.xpath('.//@data-screen-name').extract()[0]
            userId = sel.xpath('./@data-user-id').extract()[0]

            publisherInfo = {
                'username': username, 'screenName': screenName,
                'userId': userId
            }

            item['publisherInfo'] = publisherInfo

            # interactions
            item['retweets'] = int(sel.css('span.ProfileTweet-action--retweet > span.ProfileTweet-actionCount')
                .xpath('@data-tweet-stat-count').extract()[0])
            item['likes'] = int(sel.css('span.ProfileTweet-action--favorite > span.ProfileTweet-actionCount')
                .xpath('@data-tweet-stat-count').extract()[0])
            item['replies'] = int(sel.css('span.ProfileTweet-action--reply > span.ProfileTweet-actionCount')
                .xpath('@data-tweet-stat-count').extract()[0])

            # multimedia properties
            urls = []
            url_nodes = sel.css('a.twitter-timeline-link:not(.u-hidden)')
            for url_node in url_nodes:
                try:
                    urls.append(url_node.xpath('@data-expanded-url').extract()[0])
                except IndexError as err:
                    pass


            hashtags = [''.join(hashtag_node.xpath('.//text()').extract()) for hashtag_node in sel.xpath('.//a[contains(@class, "twitter-hashtag")]')]
            
            photos = []
            photo_nodes = sel.xpath('.//div[contains(@class, "AdaptiveMedia-photoContainer")]')
            for photo_node in photo_nodes:
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

            item['query'] = response.meta['query']

            if self.min_tweet is None:
                self.min_tweet = item

            yield(item)

            
        self.currentIteration += 1

        # self.state["crawl_tweets"] = self.state.get('crawl_tweets', 0) + 1
        # print("state hints1 %d " % self.state["crawl_tweets"])

        if(self.currentIteration <= self.totalIterations and len_sels > 0):
            max_tweet = item
            if self.min_tweet['tweetId'] is not max_tweet['tweetId']:
                if "min_position" in data.keys():
                    max_position = data['min_position']
                else:
                    max_position = "TWEET-%s-%s" % (max_tweet['tweetId'], self.min_tweet['tweetId'])
                print("Search hints 18: %d" % self.currentIteration)
                next_url = "https://twitter.com/i/search/timeline?f=tweets&q={}&src=typd&max_position=" + max_position+ "&reset_error_state=false"
                next_url = next_url.format(response.meta['query'])
                yield http.Request(next_url, callback=self.parse, meta={"query": response.meta['query']})