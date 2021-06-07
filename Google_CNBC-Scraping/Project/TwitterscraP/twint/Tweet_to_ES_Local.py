import hashlib
import requests
import glob
import preprocessor as tc
import pandas as pd
import numpy as np
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

# Settings for twitter tweet preprocessing #
tc.set_options(tc.OPT.URL, tc.OPT.MENTION, tc.OPT.HASHTAG, tc.OPT.RESERVED, tc.OPT.EMOJI, tc.OPT.SMILEY)



#file = r"C:\\Users\\mp094\\Desktop\\Twint Jsons\\news_2020-12-25_2020-12-29.json"
#data = [json.loads(line) for line in open(file,encoding='latin-1')]

def dictionary_key_delete(dictionary,key):
    del dictionary[key]

def hash_generator(dictionary,key):
    hashs = []

    for i in range(len(dictionary)):
        hashs.append(str(hashlib.md5(json.dumps(dictionary[i][key], sort_keys=True).encode('utf-8')).hexdigest()))

    return hashs


if __name__ == '__main__':
    files_to_process = r"C:\\Users\\mp094\\Desktop\\Twint Jsons\\*.json"

    es_client = Elasticsearch([''], http_compress=True)             # ElasticSearch Instance

    for file in glob.glob(files_to_process):


        data = [json.loads(line) for line in open(file, encoding='latin-1')]

        # Step 1 - Add timestamp field #
        for i in range(len(data)):
            timestamp = data[i]['created_at'].split(" ")[0] + " " + data[i]['created_at'].split(" ")[1]
            timestamp = (pd.to_datetime(timestamp))
            data[i]['timestamp'] = timestamp.isoformat()

        # Step 2- CLean twitter tweets #
        for i in range(len(data)):
            clean_tweet = tc.clean(data[i]['tweet'])
            data[i]['tweet'] = clean_tweet

        # Step 3 - Delete unwanted keys #
        keys = ['reply_to', 'translate', 'trans_src', 'trans_dest', 'conversation_id', 'photos', 'video']
        for key in keys:
            for i in range(len(data)):
                try:
                    dictionary_key_delete(data[i], key)
                except Exception as e:
                    pass

        hashs = hash_generator(data,'tweet')  # need to specify the dictionary and the key to generate hashs on

        processed_titles = []

        for i in range(len(data)):
            try:
                processed_titles.append(data[i]['tweet'])
            except Exception as e:
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
                "_index": "tweets",
                "_id": hashs[i],
                "_op_type": "create",
                "_source": data[i]
            })

        with open(r'C:\Users\mp094\Desktop\Twint Jsons\Clean Twitter Jsons\%s' % (file.split('\\')[-1]), 'w') as fout:
            json.dump(new_items, fout)

        print(file)
        # Section that pushes to ElasticSearch #

        #with open(r"C:\Users\mp094\Desktop\Twint Jsons\Clean Twitter Jsons\%s" %  (file.split('\\')[-1])) as json_file:
        #    data = json.load(json_file)

        #if len(data) != 0:
        #    successes, failures = helpers.bulk(es_client, data, index='tweets', stats_only=True, raise_on_error=False)

        #print(successes)
        #print(failures)
