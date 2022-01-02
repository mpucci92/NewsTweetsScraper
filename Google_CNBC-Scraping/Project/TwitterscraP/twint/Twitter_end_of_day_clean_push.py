import hashlib
import requests
import glob
import preprocessor as tc
import pandas as pd
import numpy as np
import json
import logging

# Settings for twitter tweet preprocessing #
tc.set_options(tc.OPT.URL, tc.OPT.MENTION, tc.OPT.HASHTAG, tc.OPT.RESERVED, tc.OPT.EMOJI, tc.OPT.SMILEY)

def Twitterclean_logger():
    global logger_twittercleaner
    logger_twittercleaner = logging.getLogger(__name__)
    logger_twittercleaner.setLevel(logging.DEBUG)

    fh = logging.FileHandler(f"/home/learn_compoundguru/Google_CNBC-Scraping/Project/TwitterscraP/logs/twitter_clean.log")
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger_twittercleaner.addHandler(fh)

    logger_twittercleaner.info('--------------START OF LOG FILE------------------')
    logger_twittercleaner.info('-------------TWITTER CLEANER LOGS----------------')



def dictionary_key_delete(dictionary,key):
    del dictionary[key]

def hash_generator(dictionary,key):
    hashs = []

    for i in range(len(dictionary)):
        hashs.append(str(hashlib.md5(json.dumps(dictionary[i][key], sort_keys=True).encode('utf-8')).hexdigest()))

    return hashs


if __name__ == '__main__':
    files_to_process = "/home/learn_compoundguru/Google_CNBC-Scraping/Project/TwitterscraP/data/twitter_raw/*.json"               # Location where raw twitter file is stored
    
    Twitterclean_logger()

    for file in glob.glob(files_to_process):


        data = [json.loads(line) for line in open(file, encoding='latin-1')]

        # Step 1 - Add timestamp field #
        for i in range(len(data)):
            try:
                timestamp = data[i]['created_at'].split(" ")[0] + " " + data[i]['created_at'].split(" ")[1]
                timestamp = (pd.to_datetime(timestamp))
                data[i]['timestamp'] = timestamp.isoformat()
            except:
                logger_twittercleaner.info("Error in Timestamp field") 

        # Step 2- Clean twitter tweets #
        for i in range(len(data)):
            try:
                clean_tweet = tc.clean(data[i]['tweet'])
                data[i]['tweet'] = clean_tweet
            except:
                logger_twittercleaner.info("Error in twitter cleaning field")

        # Step 3 - Delete unwanted keys #
        keys = ['reply_to', 'translate', 'trans_src', 'trans_dest', 'conversation_id', 'photos', 'video']
        for key in keys:
            for i in range(len(data)):
                try:
                    dictionary_key_delete(data[i], key)
                except Exception as e:
                    logger_twittercleaner.info("Error in deleting dictionary key field")
                    pass

        hashs = hash_generator(data,'tweet')  # need to specify the dictionary and the key to generate hashs on

        processed_titles = []

        for i in range(len(data)):
            try:
                processed_titles.append(data[i]['tweet'])
            except Exception as e:
                logger_twittercleaner.info("Error in appending titles to processed list")
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
                "_index": "tweets",  # Change to news 
                "_id": hashs[i],
                "_op_type": "create",
                "_source": data[i]    # clean_tweet(data[i])
            })

        # Location to push the Cleaned file too #
        with open('/home/learn_compoundguru/Google_CNBC-Scraping/Project/TwitterscraP/data/twitter_clean/%s' % (file.split('/')[-1]), 'w',encoding='latin-1') as fout:
            try:
                json.dump(new_items, fout)
            except:
                logger_twittercleaner.info(f"Error with {file}")

        
