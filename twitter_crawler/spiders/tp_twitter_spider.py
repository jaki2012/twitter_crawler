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

from twitter_crawler.items import TwitterCrawlerItem
CUSTOMIZED_DEBUG = False

class TwitterSpider(scrapy.Spider):
    
    name = "tptwitter"
    bloomfilter = BloomFilter()

    def __init__(self, query=None, *args ,**kwargs):

        super(TwitterSpider, self).__init__(*args, **kwargs)


        self.start_urls = ['https://twitter.com/i/profiles/show/POTUS/timeline/tweets?include_available_features=1&include_entities=1&max_position=991337476177453058&reset_error_state=false']
        self.queries = []
        self.job_dir = ''
        self.temp_query = None
        self.duplicated_num = 0
        self.expire = False
        self.expire_time = datetime.strptime('2018-03-01 00:00:00', '%Y-%m-%d %H:%M:%S')

    def parse(self, response):
        data = json.loads(response.body.decode("utf-8"))
        response_selec = Selector(text=data['items_html'])
        # sels = response.xpath('.//div[@class="stream"]/ol[contains(@class, "stream-items")]/li[contains(@class, "stream-item")]')
        sels = response_selec.xpath('//li[@data-item-type="tweet"]')
        len_sels = len(sels)
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

                print(item['text'])
                item['conversationId'] = sel.xpath('.//@data-conversation-id').extract()[0]
                temp = sel.xpath('.//div[@class="context"]//span[contains(@class, "Icon--retweeted")]').extract()
                if len(temp) > 0:
                    item['retweeted'] = True
                else:
                    item['retweeted'] = False

                if item['text'] == '':
                    # If there is not text, we ignore the tweet
                    continue

                unix_time = int(sel.xpath('.//span[contains(@class, "_timestamp")]/@data-time').extract()[0])
                item['time'] = datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')
                print(item['time'])
                print(self.expire_time)
                item['time'] = datetime.strptime(item['time'], '%Y-%m-%d %H:%M:%S')
                if item['time'] < self.expire_time:
                    self.expire = True
                    break

                # information of the publisher
                publisherInfo = {}
                username = sel.xpath('.//@data-name').extract()[0]
                screenName = "@" + sel.xpath('.//@data-screen-name').extract()[0]
                userId = sel.xpath('.//@data-user-id').extract()[0]
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
                url_nodes = sel.css('a.twitter-timeline-link:not(.u-hidden)')
                for url_node in url_nodes:
                    try:
                        urls.append(url_node.xpath('@data-expanded-url').extract()[0])
                    except IndexError as err:
                        print('index error.')
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
                            else:
                                video_id = tmp[:tmp.index('.png')]
                            videos.append({'id': video_id})
                if CUSTOMIZED_DEBUG:
                    print(videos)

                entries = {
                    'hashtags': hashtags, 'photos': photos,
                    'urls': urls, 'videos': videos
                }

                item['entries'] = entries


                yield(item)
            except Exception as err:
                print('trace_back before')
                traceback.print_exc()
                # print("Error happens when parse tweet:\n%s" % sel.xpath('.').extract()[0])

        if(len_sels > 0 and not self.expire):
            max_tweet = item
            if min_tweet_id is not max_tweet['tweetId']:
                if "min_position" in data.keys():
                    max_position = data['min_position']
                next_url = 'https://twitter.com/i/profiles/show/realDonaldTrump/timeline/tweets?include_available_features=1&include_entities=1&max_position=' + max_position +'&reset_error_state=false'
                if CUSTOMIZED_DEBUG: 
                    print("requesting \n%s" % next_url)
                yield http.Request(next_url, callback=self.parse)