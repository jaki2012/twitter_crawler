import scrapy
from scrapy import http
import traceback
from scrapy.selector import Selector
from datetime import datetime
from twitter_crawler.bloomfilter import BloomFilter
import json
import os

from twitter_crawler.items import TwitterCrawlerItem

class TwitterSpider(scrapy.Spider):
    
    name = "twitter"
    bloomfilter = BloomFilter()

    def __init__(self, query=None, *args ,**kwargs):

        super(TwitterSpider, self).__init__(*args, **kwargs)

        self.url_base = 'https://twitter.com/i/search/timeline?f=tweets&src=typd&q={}'
        self.start_urls = []
        self.queries = []
        self.temp_query = None
        self.duplicated_num = 0


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

        self.currentIteration = 1
        # max_iterations
        # always execute 2 more requests
        self.totalIterations = 500000
        self.min_tweet = None
        # crawled_starturls_index
        self.crawled_stuidx = -1

        # To verify if each start_url corresponds to its original query
        for i, start_url in enumerate(self.start_urls):
            print("%s with %s" % (self.queries[i], start_url))

    def start_requests(self):
        if hasattr(self, 'state'):
            self.currentIteration = self.state.get('iteration_num', 1)
            if self.currentIteration > self.totalIterations:
                print("The job has been finished, hence we do nothing here.")
                return
            self.crawled_stuidx = self.state.get('crawled_stuidx', -1)
        print("Start from iterations %d" % self.currentIteration)
        for i, url in enumerate(self.start_urls):
            if i <= self.crawled_stuidx:
                continue
            print("start requesting \n%s" % url)
            # set dont_filter to be True means we allowing duplicating on this url
            yield http.Request(url, dont_filter=True, meta={"query": self.queries[i]})
            if hasattr(self, 'state'):
                self.state['crawled_stuidx'] = self.state.get('crawled_stuidx', -1) + 1

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
        # print(dir(response))
        # print(response.url)
        # original当时也有开发接口调用次数限制
        data = json.loads(response.body.decode("utf-8"))
        response_selec = Selector(text=data['items_html'])
        # sels = response.xpath('.//div[@class="stream"]/ol[contains(@class, "stream-items")]/li[contains(@class, "stream-item")]')
        sels = response_selec.xpath('//li[@data-item-type="tweet"]/div')
        len_sels = len(sels)
        for i, sel in enumerate(sels):
            try:
                item = TwitterCrawlerItem()
                
                # core parts of tweet
                tweetId = sel.xpath('.//a[contains(@class, "js-permalink")]/@data-conversation-id').extract()

                # to avoid crashing because of twitter's returnning zero values
                if not tweetId:
                    continue
                else:
                    item['tweetId'] = tweetId[0]
                # BloomFilter
                # if self.bloomfilter.isContains(tweetId.encode("utf-8")):
                #     print("Dublicate tweet with id %s found." % tweetId)
                #     self.duplicated_num +=1
                #     continue
                # else:
                #     self.bloomfilter.insert(tweetId.encode("utf-8"))
                #     item['tweetId'] = tweetId

                item['text'] = ''.join(sel.xpath('.//div[@class="js-tweet-text-container"]/p//text()').extract())

                if item['text'] == '':
                    # If there is not text, we ignore the tweet
                    continue

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
                retweets = sel.css('span.ProfileTweet-action--retweet > span.ProfileTweet-actionCount').xpath('@data-tweet-stat-count').extract()
                if retweets:
                    item['retweets'] = int(retweets[0])
                else:
                    item['retweets'] = 0

                likes = sel.css('span.ProfileTweet-action--favorite > span.ProfileTweet-actionCount').xpath('@data-tweet-stat-count').extract()
                if likes:
                    item['likes'] = int(likes[0])
                else:
                    item['likes'] = 0

                replies = sel.css('span.ProfileTweet-action--reply > span.ProfileTweet-actionCount').xpath('@data-tweet-stat-count').extract()
                if replies:
                    item['replies'] = int(replies[0])
                else:
                    item['replies'] = 0

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
            except Exception as err:
                traceback.print_exc()
                print("Error happens when parse tweet:\n%s" % sel.xpath('.').extract()[0])

        if(self.currentIteration <= self.totalIterations and len_sels > 0):
            max_tweet = item
            if self.min_tweet['tweetId'] is not max_tweet['tweetId']:
                if "min_position" in data.keys():
                    max_position = data['min_position']
                else:
                    max_position = "TWEET-%s-%s" % (max_tweet['tweetId'], self.min_tweet['tweetId'])
                print("Current iterations: %d" % self.currentIteration)
                next_url = "https://twitter.com/i/search/timeline?f=tweets&q={}&src=typd&max_position=" + max_position+ "&reset_error_state=false"
                next_url = next_url.format(response.meta['query'])
                self.currentIteration += 1
                if hasattr(self, 'state'):
                    self.state['iteration_num'] = self.state.get('iteration_num', 0) + 1
                # print("requesting \n%s" % next_url)
                yield http.Request(next_url, callback=self.parse, meta={"query": response.meta['query']})