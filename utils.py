import codecs
import tqdm
import redis
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

    a = db.tweets.aggregate([{
       '$group':
         {
           '_id':  '$tweetId' ,
           # 'queries': { '$addToSet': { "$substr": [ "$query", 0, { '$subtract': [ { '$strLenCP': "$query" }, 17 ] }]} },
           'text': { '$first':'$text'},
           'time': { '$first':'$time'},
           'userName': { '$first':'$publisherInfo.username'},
           'userScreenName':{ '$first':'$publisherInfo.screenName'},
           'userId' :{ '$first':'$publisherInfo.userId'},
           'numberRetweets' : {'$first':'$retweets'},
           'numberLikes' : {'$first':'$likes'},
           'numberReplies' : {'$first':'$replies'},
           # 'hashtags': {'$first':'$entries.hashtags'},
           # 'photos': {'$first':'$entries.photos'},
           # 'urls': {'$first':'$entries.urls'},
           # 'videos': {'$first':'$entries.videos'}
         }
     },{
        '$out':'unique_tweets'
     }])

    print("finished!")



if __name__ == '__main__':
    # generate_queries()
    remove_duplicate()
    # get_queries_nums()