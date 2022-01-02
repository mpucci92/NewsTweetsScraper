# Libraries #
import twint
from datetime import datetime, timedelta
import logging
from time import gmtime, strftime, localtime
import glob as glob
import json
import requests
import hashlib
import requests
import glob
import preprocessor as tc
import pandas as pd
import numpy as np
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

# Variables #
date = (datetime.today().strftime('%Y-%m-%d'))
localtime = strftime("%Y-%m-%d_%H-%M-%S", localtime())


users = ['CNBC', 'Benzinga', 'Business', 'nytimesbusiness', 'ReutersMoney', 'ReutersBiz','stockhouse',
         'WSJmarkets', 'barronsonline', 'businessinsider', 'Forbes', 'MarketWatch', 'BusinessWire',
         'CNNBusiness', 'MorningstarInc', 'PRNewswire', 'YahooFinance', 'Deltaone', 'zerohedge','crypto', 
         'ForbesCrypto','BTCTN','CoinMarketCap','cointelegraph','crediblecrypto']

# Settings for twitter tweet preprocessing #
tc.set_options(tc.OPT.URL, tc.OPT.MENTION, tc.OPT.HASHTAG, tc.OPT.RESERVED, tc.OPT.EMOJI, tc.OPT.SMILEY)

def dictionary_key_delete(dictionary,key):
    del dictionary[key]

def hash_generator(dictionary,key):
    hashs = []

    for i in range(len(dictionary)):
        hashs.append(str(hashlib.md5(json.dumps(dictionary[i][key], sort_keys=True).encode('utf-8')).hexdigest()))

    return hashs

# Logger for merging data #
def Twitterscraper_live_logger():
    global logger_twitterscraper_live
    logger_twitterscraper_live = logging.getLogger(__name__)
    logger_twitterscraper_live.setLevel(logging.DEBUG)

    fh = logging.FileHandler(f"/home/learn_compoundguru/Google_CNBC-Scraping/Project/TwitterscraP/logs/twitter_raw_live.log")
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger_twitterscraper_live.addHandler(fh)

    logger_twitterscraper_live.info('--------------START OF LOG FILE------------------')
    logger_twitterscraper_live.info('-------------TWITTER SCRAPER LOGS----------------')

if __name__ == '__main__':
    es_client = Elasticsearch([''], http_compress=True)

    data_path = f"/home/learn_compoundguru/Google_CNBC-Scraping/Project/TwitterscraP/data/twitter_raw_live/news_{localtime}.json"
    data_path_process = "/home/learn_compoundguru/Google_CNBC-Scraping/Project/TwitterscraP/data/twitter_raw_live/*.json"
    c = twint.Config()

    Twitterscraper_live_logger()

    # counter for the log files
    counter = 0

    for name in users:
        c.Lang = 'en'
        c.Username = name
        c.Limit = 100
        c.Store_json = True
        c.Output = data_path  # Location to save raw twitter data

        try:
            twint.run.Search(c)
            logger_twitterscraper_live.info(f"Complete:{name}")
            counter = counter + 1
            logger_twitterscraper_live.info("Percentage completed:")
            logger_twitterscraper_live.info((counter / len(users) * 100))

        except:
            logger_twitterscraper_live.info(f"Error Processing Twitter Handle:{name}")


    for file in glob.glob(data_path_process):


        data = [json.loads(line) for line in open(file, encoding='latin-1')]

        # Step 1 - Add timestamp field #
        for i in range(len(data)):
            try:
                timestamp = data[i]['created_at'].split(" ")[0] + " " + data[i]['created_at'].split(" ")[1]
                timestamp = (pd.to_datetime(timestamp))
                data[i]['timestamp'] = timestamp.isoformat()
            except:
                logger_twitterscraper_live.info("Error in Timestamp field")

                # Step 2- Clean twitter tweets #
        for i in range(len(data)):
            try:
                clean_tweet = tc.clean(data[i]['tweet'])
                data[i]['tweet'] = clean_tweet
            except:
                logger_twitterscraper_live.info("Error in twitter cleaning field")

        # Step 3 - Delete unwanted keys #
        keys = ['reply_to', 'translate', 'trans_src', 'trans_dest', 'conversation_id', 'photos', 'video']
        for key in keys:
            for i in range(len(data)):
                try:
                    dictionary_key_delete(data[i], key)
                except Exception as e:
                    logger_twitterscraper_live.info("Error in deleting dictionary key field")


        hashs = hash_generator(data,'tweet')  # need to specify the dictionary and the key to generate hashs on

        processed_titles = []

        for i in range(len(data)):
            try:
                processed_titles.append(data[i]['tweet'])
            except Exception as e:
                logger_twitterscraper_live.info("Error in appending titles to processed list")
                pass

        headers = {"Content-Type": "application/json"}
        data_sentiment = {"sentences": processed_titles}

        response = requests.get("http://localhost:9602/predict", headers=headers, json=data_sentiment)

        sentiment = []
        sentiment_score = []

        for i in range(len(json.loads(response.content))):
            sentiment.append((json.loads(response.content))[str(i)]['prediction'])
            sentiment_score.append((json.loads(response.content))[str(i)]['sentiment_score'])

        for i in range(len(data)):
            data[i]['sentiment'] = sentiment[i]
            data[i]['sentiment_score'] = sentiment_score[i]


        new_items = []

        for i in range(len(data)):
            new_items.append({
                "_index": "tweets",   # Change this to news
                "_id": hashs[i],
                "_op_type": "create",
                "_source": data[i]    # clean_tweet(data[i])
            })

        # Section that pushes to ElasticSearch #

        if len(new_items) != 0:
            try:
                successes, failures = helpers.bulk(es_client, new_items, index='tweets', stats_only=True, raise_on_error=False)
                logger_twitterscraper_live.info(f"Successful Document Count: {successes}")
                logger_twitterscraper_live.info(f"Failed Document Count: {failures}")
                logger_twitterscraper_live.info("Successful bulk push to ES: %s " % (file))

            except Exception as e:
                logger_twitterscraper_live.debug("Error with file: %s" % (file))

           
