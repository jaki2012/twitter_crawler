import codecs
# import tqdm
from tqdm import tqdm
import redis
import json
import time
import traceback
import pymysql
import pymongo
from twitter_crawler import settings
from multiprocessing.dummy import Pool as ThreadPool

def generate_queries(keywords_file_name='keywords.txt', output_file_name='queries.txt'):
    file = codecs.open(keywords_file_name, 'r', 'utf-8')
    # file to output generated queries
    output = codecs.open(output_file_name, 'w', 'utf-8')

    lines = [line.strip() for line in file]

    queries = []
    for line in lines:
        determiners, phases = line.split(':')
        for phase in phases.split(','):
            queries.append(phase + ' ' + ' OR '.join(determiners.split(',')))

    output.write(','.join(queries))

def get_queries_nums(query_file='queries.txt'):
    file = codecs.open(query_file, 'r', 'utf-8')

    queries = file.readlines()[0]
    print(len(queries.split(',')))
            

def remove_duplicate():
    client = pymongo.MongoClient(host=settings.MONGO_HOST, port=settings.MONGO_PORT)
    client.admin.authenticate(settings.MONGO_USER, settings.MONGO_PASSWORD)
    db = client[settings.MONGO_DB]
    collection = db[settings.MONGO_COLLECTION]

    date_query_item_len = len(' since:2018-03-01')

    tweets = db.tweets.aggregate([{
       '$group':
         {
           '_id':  '$tweetId' ,
           'queries': { '$addToSet': { "$substr": [ "$query", 0, { '$subtract': [ { '$strLenCP': "$query" }, 17 ] }]} },
           'text': { '$first':'$text'},
           'time': { '$first':'$time'},
           'userName': { '$first':'$publisherInfo.username'},
           'userScreenName':{ '$first':'$publisherInfo.screenName'},
           'userId' :{ '$first':'$publisherInfo.userId'},
           'numberRetweets' : {'$first':'$retweets'},
           'numberLikes' : {'$first':'$likes'},
           'numberReplies' : {'$first':'$replies'},
           'hashtags': {'$first':'$entries.hashtags'},
           'photos': {'$first':'$entries.photos'},
           'urls': {'$first':'$entries.urls'},
           'videos': {'$first':'$entries.videos'}
         }
     }])

    sql_db = pymysql.connect("localhost", "root", "1234", "twitter_crawler", charset='utf8')
    cursor = sql_db.cursor()
    insert_tweet_sqltemplate = "INSERT INTO TWITTER(TWEET_ID, TEXT, TIME, USER_NAME, USER_SCREEN_NAME, USER_ID, NUMBER_RETWEETS, NUMBER_LIKES, NUMBER_REPLIES) VALUES('%d', '%s', '%s', '%s', '%s', '%d', '%d', '%d', '%d')".lower()
    insert_query_sqltemplate = "INSERT INTO TWITTER_QUERY(TWEET_ID, QUERY) VALUES ('%d', '%s')".lower()
    insert_hashtag_sqltemplate = "INSERT INTO TWITTER_HASHTAG(TWEET_ID, HASHTAG) VALUES ('%d', '%s')".lower()
    insert_photo_sqltemplate = "INSERT INTO TWITTER_PHOTO(TWEET_ID, PHOTO_URL) VALUES ('%d', '%s')".lower()
    insert_video_sqltemplate = "INSERT INTO TWITTER_VIDEO(TWEET_ID, VIDEO_ID) VALUES ('%d', '%s')".lower()
    insert_url_sqltemplate = "INSERT INTO TWITTER_URL(TWEET_ID, URL) VALUES ('%d', '%s')".lower()

    tweets_list = list(tweets)
    tweets = tqdm(tweets_list)
    for tweet in tweets:
      print(tweet['videos'])
      tweet['_id'] = int(tweet['_id'])
      tweet['userId'] = int(tweet['userId'])
      tweet['text'] = pymysql.escape_string(tweet['text'])
      tweet['userName'] = pymysql.escape_string(tweet['userName'])
      try:
        # 执行sql语句
        cursor.execute(insert_tweet_sqltemplate % (tweet['_id'], tweet['text'], tweet['time'], tweet['userName'], tweet['userScreenName'], tweet['userId'],
          int(tweet['numberRetweets']), int(tweet['numberLikes']), int(tweet['numberReplies'])))

        for query in tweet['queries']:
          query = pymysql.escape_string(query)
          cursor.execute(insert_query_sqltemplate % (tweet['_id'], query))

        for hashtag in tweet['hashtags']:
          hashtag = pymysql.escape_string(hashtag)
          cursor.execute(insert_hashtag_sqltemplate % (tweet['_id'], hashtag))

        for photo in tweet['photos']:
          photo = pymysql.escape_string(photo)
          cursor.execute(insert_photo_sqltemplate % (tweet['_id'], photo))

        for video in tweet['videos']:
          video = pymysql.escape_string(video)
          cursor.execute(insert_video_sqltemplate % (tweet['_id'], video))

        for url in tweet['urls']:
          url = pymysql.escape_string(url)
          cursor.execute(insert_url_sqltemplate % (tweet['_id'], url))
        # 提交到数据库执行
        sql_db.commit()
      except:
        traceback.print_exc()
        # 发生错误时回滚
        sql_db.rollback()

def sma_analyze():
    good_words_file = codecs.open('good.txt', 'r', 'utf-8')
    line = good_words_file.readlines()[0].strip('\n')
    good_words = [word.strip(' ').lower() for word in line.split(',')]

    print(good_words)
    
    bad_words_file = codecs.open('bad.txt', 'r', 'utf-8')
    line = bad_words_file.readlines()[0].strip('\n')
    bad_words = [word.strip(' ').lower() for word in line.split(',')]
    print(bad_words)

    sql_db = pymysql.connect("localhost", "root", "1234", "twitter_crawler", charset='utf8')
    cursor = sql_db.cursor()
    query_sql = "SELECT * FROM twitter"
    update_sql = "UPDATE twitter SET emotion_score = %f , positive_score = %f, negative_score = %f WHERE tweet_id = %d"
    try:
      # 执行SQL语句
      cursor.execute(query_sql)
      # 获取所有记录列表
      results = cursor.fetchall()
      results = tqdm(results)
      for row in results:
        tweet_id = row[0]
        text = row[1].lower()
        positive_score = 0.0
        negative_score = 0.0
        for good_word in good_words:
          if good_word in text:
            positive_score = positive_score + 1
        for bad_word in bad_words:
          if bad_word in text:
            negative_score = negative_score + 1
        # print(tweet_id, " === ",emotion)
        cursor.execute(update_sql %(positive_score-negative_score, positive_score, negative_score, tweet_id))
        # print(update_sql %(positive_score+negative_score, positive_score, negative_score, tweet_id))
        sql_db.commit()
    except:
      print("error")
      sql_db.rollback()

if __name__ == '__main__':
    # generate_queries()
    # remove_duplicate()
    sma_analyze()
    # get_queries_nums()