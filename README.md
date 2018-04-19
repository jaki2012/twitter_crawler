# twitter_crawler

### Usage:
##### We provide a quick entry to execute the twitter-crawler:
```
scrapy crawl twitter -a file=queries.txt -a language=en -a since=2018-03-01
```
##### or more robustly:
```
scrapy crawl twitter -a file=queries.txt -a language=en -a since=2018-03-01 -s JOBDIR=crawls/twitter-001
```