import scrapy
import redis
import time
from scrapy import http
from scrapy.conf import settings
import traceback
from scrapy.selector import Selector
from datetime import datetime
from twitter_crawler.bloomfilter import BloomFilter
import json
import os
import re

from urllib.parse import quote

from twitter_crawler.items import TwitterCrawlerItem
CUSTOMIZED_DEBUG = False

class TwitterSpider(scrapy.Spider):
    
    name = "twitter"
    bloomfilter = BloomFilter()

    def __init__(self, query=None, *args ,**kwargs):

        super(TwitterSpider, self).__init__(*args, **kwargs)
        self.url_base = 'https://twitter.com/search?src=typd&q={}'
        self.next_url_base = 'https://twitter.com/i/search/timeline?vertical=default&q={}&l=en&src=typd&{}_position={}&reset_error_state=false'
        self.start_urls = []
        self.queries = []
        # A dict to record whether the initital request of a specific query has already been processed, or not
        self.inited_dict = {}
        self.job_dir = ''
        self.temp_query = None
        self.inited = False
        self.duplicated_num = 0
        self.redis_server = redis.Redis(host='localhost', password='pandora', port=6379, decode_responses=True)

        if kwargs.get('file') is not None:
            self.get_conversations = True

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
                print(self.start_urls)
                self.queries.append(self.temp_query)

        self.currentIteration = 1
        # max_iterations, always execute 2 more requests
        self.totalIterations = 500000
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
            
            # necessary headers to get json response from twitter
            headers = {
                "Accept" : "application/json, text/javascript, */*; q=0.01",
                "x-push-state-request" : "true",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "en"
            }
            # set dont_filter to be True means we allowing duplicating on this url
            yield http.Request(url, method='GET', headers=headers, dont_filter=True, meta={"query": self.queries[i]})
            if hasattr(self, 'state'):
                self.state['crawled_stuidx'] = self.state.get('crawled_stuidx', -1) + 1

    def construct_url(self, query, **kwargs):
        constructed_url = self.url_base
        if kwargs.get("since") is not None:
            query += (" since:" + kwargs.get("since"))
        if kwargs.get("until") is not None:
            query += (" until:" + kwargs.get("until"))
        constructed_url  = self.url_base.format(quote(query, 'utf-8'))
        if kwargs.get('language') is not None:
                constructed_url  += ("&l=" + kwargs.get("language"))
        self.temp_query = query
        return constructed_url

    def get_min_tweetId(self, item):
        if settings['JOBDIR'] is not None:
            query_key = '-'.join([settings['JOBDIR'] , item['query']])
        else:
            query_key = item['query']
        tweetId = self.redis_server.get(query_key)
        if tweetId is None:
            self.redis_server.set(query_key, item['tweetId'])
            if CUSTOMIZED_DEBUG:
                print("Get mintweetid return from insert.")
            return item['tweetId']
        else:
            if CUSTOMIZED_DEBUG:
                print("Get mintweetid from query.")
            return tweetId



    def parse(self, response):

        data = json.loads(response.body.decode("utf-8"))
        # which query_word is we are requesting
        query_word = response.meta['query']
        if(query_word not in self.inited_dict.keys()): 
            # To process init request first
            # Current iterations 0
            init_data = data['page']
            matchObj = re.search(r'data-max-position=\"(.*?)\"', init_data)
            next_url = self.next_url_base.format(quote(response.meta['query'], 'utf-8'), 'max', quote(matchObj.group(1), 'utf-8'))

            response_selec = Selector(text=data['page'])
            
        else:
            response_selec = Selector(text=data['items_html'])
        
        # sels = response.xpath('.//div[@class="stream"]/ol[contains(@class, "stream-items")]/li[contains(@class, "stream-item")]')
        sels = response_selec.xpath('//li[@data-item-type="tweet"]/div')

        len_sels = len(sels)
        if CUSTOMIZED_DEBUG: 
            print("return %d tweets for query:%s " % (len_sels, response.meta['query']))
        min_tweet_id = None
        for i, sel in enumerate(sels):
            try:
                item = TwitterCrawlerItem()              
                # core parts of tweet
                tweetId = sel.xpath('.//@data-tweet-id').extract()

                # to avoid crashing the program because of twitter's returnning zero values
                if not tweetId:
                    continue
                else:
                    if settings['BLOOMFILTER_ENABLED']:
                        print('Bloomfilter enabled.')
                        # BloomFilter
                        if self.bloomfilter.isContains(tweetId[0].encode("utf-8")):
                            print("Dublicate tweet with id %s found." % tweetId[0])
                            self.duplicated_num +=1
                            continue
                        else:
                            self.bloomfilter.insert(tweetId[0].encode("utf-8"))
                            item['tweetId'] = tweetId[0]
                    else:
                        item['tweetId'] = tweetId[0]

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
                verified = sel.xpath('.//span[@class="FullNameGroup"]//span[contains(@class, "Icon--verified")]')
                if len(verified) > 0:
                    verified = True
                else:
                    verified = False


                publisherInfo = {
                    'username': username, 'screenName': screenName,
                    'userId': userId, 'verified':verified
                }

                item['publisherInfo'] = publisherInfo

                # interactions
                retweets = sel.css('span.ProfileTweet-action--retweet > span.ProfileTweet-actionCount').xpath(
                    '@data-tweet-stat-count').extract()
                if retweets:
                    item['retweets'] = int(retweets[0])
                else:
                    item['retweets'] = 0

                likes = sel.css('span.ProfileTweet-action--favorite > span.ProfileTweet-actionCount').xpath(
                    '@data-tweet-stat-count').extract()
                if likes:
                    item['likes'] = int(likes[0])
                else:
                    item['likes'] = 0

                replies = sel.css('span.ProfileTweet-action--reply > span.ProfileTweet-actionCount').xpath(
                    '@data-tweet-stat-count').extract()
                if replies:
                    item['replies'] = int(replies[0])
                else:
                    item['replies'] = 0

                # multimedia properties
                urls = []
                # url_nodes = sel.css('a.twitter-timeline-link:not(.u-hidden)')
                # css selectors for elements :<a class='twitter-timeline-link' data-expane-url='0'> </a>
                url_nodes = sel.css('a.twitter-timeline-link[data-expanded-url]')
                # some urls like before selected by previous css selector cause exception
                # <a href="https://t.co/AfqkVnBVpE" class="twitter-timeline-link" data-pre-embedded="true" dir="ltr">pic.twitter.com/AfqkVnBVpE</a>
                for url_node in url_nodes:
                    # Modified by jiechu li, in 2018.11.02, to solve twitter front-end modification: https://t.co/KOtfDWMP3R
                    try:
                        urls.append(url_node.xpath('.//@data-expanded-url').extract()[0])
                    except IndexError as err:
                        print('index error.')
                        if CUSTOMIZED_DEBUG:
                            # see the modification on this selector
                            print(url_node.extract())
                            print("================")
                        pass

                hashtags = [''.join(hashtag_node.xpath('.//text()').extract()) for hashtag_node in sel.xpath('.//a[contains(@class, "twitter-hashtag")]')]
                
                photos = []
                photo_nodes = sel.xpath('.//div[contains(@class, "AdaptiveMedia-photoContainer")]')
                for photo_node in photo_nodes:
                    photos.append(photo_node.xpath("@data-image-url").extract()[0])

                videos = []
                video_node_styles = sel.xpath('.//div[contains(@class, "PlayableMedia-player")]/@style').extract()
                
                if len(video_node_styles) > 0 :
                    video_node_styles = video_node_styles[0].split('; ')
                    for style in video_node_styles:
                        if style.startswith('background'):
                            if CUSTOMIZED_DEBUG:
                                print(style)
                            tmp = style.split('/')[-1]
                            if '.jpg' in tmp:
                                video_id = tmp[:tmp.index('.jpg')]
                            elif '.png' in tmp:
                                video_id = tmp[:tmp.index('.png')]
                            else :
                                # style = background-image:url('https://pbs.twimg.com/card_img/1056912735789817856/kYIEOewj?format=jpg&name=280x280')
                                # recorded in 2018.11.01
                                tmp = style.split('/')[-2]
                                video_id = tmp
                            videos.append({'id': video_id})
                if CUSTOMIZED_DEBUG:
                    print(videos)

                entries = {
                    'hashtags': hashtags, 'photos': photos,
                    'urls': urls, 'videos': videos
                }

                item['entries'] = entries

                # record which query is used to catch this tweet
                item['query'] = response.meta['query']

                if min_tweet_id is None:
                    min_tweet_id = self.get_min_tweetId(item)

                yield(item)
            except Exception as err:
                print('trace_back before')
                traceback.print_exc()
                # print("Error happens when parse tweet:\n%s" % sel.xpath('.').extract()[0])


        if(query_word not in self.inited_dict.keys()):
            self.inited_dict[query_word] = 1
            if CUSTOMIZED_DEBUG: 
                print("requesting init url \n%s" % next_url)
            yield http.Request(next_url, callback=self.parse, meta={"query": response.meta['query']})
        elif(self.currentIteration <= self.totalIterations and len_sels > 0):                
            max_tweet = item
            if min_tweet_id is not max_tweet['tweetId']:
                if "min_position" in data.keys():
                    max_position = data['min_position']
                else:
                    return
            else:
                return
            
            print("Current iterations: %d" % self.currentIteration)
            next_url = self.next_url_base.format(quote(response.meta['query'], 'utf-8'), 'max', quote(max_position, 'utf-8'))
            self.currentIteration += 1
            if hasattr(self, 'state'):
                self.state['iteration_num'] = self.state.get('iteration_num', 0) + 1
            if CUSTOMIZED_DEBUG: 
                print("requesting \n%s" % next_url)
            yield http.Request(next_url, callback=self.parse, meta={"query": response.meta['query']})
