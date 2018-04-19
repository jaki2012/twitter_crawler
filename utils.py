import codecs
import tqdm
import pymongo
from scrapy.conf import settings
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

def remove_duplicate():
    client = pymongo.MongoClient(host=settings['MONGO_HOST'], port=settings['MONGO_PORT'])
    client.admin.authenticate(settings['MONGO_USER'], settings['MONGO_PASSWORD'])
    db = client[settings['MONGO_DB']]
    collection = db[settings['MONGO_COLLECTION']]
    unique_collection = db.unique_tweets

    date_query_item_len = len(' since:2018-03-01')

    max_len = 0

    distinct_tweetIds = collection.distinct('tweetId')
    def insert_unique_tweets(tweetId):
        nonlocal max_len
        queries = set()
        for tweet in collection.find({'tweetId':tweetId}):
            queries.add(tweet['query'][:-date_query_item_len])
        if len(queries)>max_len:
            max_len= len(queries)

    pool = ThreadPool()
    for _ in tqdm.tqdm(pool.imap_unordered(insert_unique_tweets, distinct_tweetIds), total=len(distinct_tweetIds)):
        pass

    pool.close()
    pool.join()

    print(max_len)



if __name__ == '__main__':
    # generate_queries()
    remove_duplicate()