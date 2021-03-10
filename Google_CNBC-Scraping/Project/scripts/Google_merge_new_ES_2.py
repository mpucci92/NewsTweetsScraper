import hashlib
import numpy as np
import pandas as pd
import json
import requests
import glob
import logging

def Google_ES_clean_logger():
    global logger_google_ES_clean
    logger_google_ES_clean = logging.getLogger(__name__)
    logger_google_ES_clean.setLevel(logging.DEBUG)

    fh = logging.FileHandler("/home/learn_compoundguru/Google_CNBC-Scraping/Project/ES_data/ES_logs/google_clean_es_logs.log")
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger_google_ES_clean.addHandler(fh)

    logger_google_ES_clean.info('---START OF THE FILE---')
    logger_google_ES_clean.info('---GOOGLE TO CLEAN ES LOGS---')



def load_data(data_path):
    df = pd.read_csv(data_path)

    df = df.loc[:, ['title', 'link', 'timestamp', 'authors', 'source_href',
                    'search_query', 'ticker_tags']]

    df.columns = ['title', 'link', 'timestamp', 'authors', 'source_href', 'search_query', 'tickers']

    df.timestamp = (pd.to_datetime(df.timestamp))
    df.timestamp = df['timestamp'].dt.tz_localize(None)

    iso_dates = []
    for i in range(len(df)):
        iso_dates.append(df.timestamp.iloc[i].isoformat())

    df.timestamp = iso_dates

    return df


def data_generator(df):
    list_dict = []

    for index, row in list(df.iterrows()):
        list_dict.append(dict(row))

    for i in range(len(list_dict)):
        list_dict[i]['tickers'] = eval(list_dict[i]['tickers'])

    list_of_dicts = []

    for i in range(len(list_dict)):
        res = {key: value for key, value in list_dict[i].items() if len(value) > 0}
        list_of_dicts.append(res)

    return list_of_dicts


def processing_titles(list_of_dicts):
    titles = []
    for i in range(len(list_of_dicts)):
        titles.append(list_of_dicts[i]['title'])

    processed_titles = []
    for i in range(len(titles)):
        processed_titles.append(titles[i].rsplit('-', 1)[0])

    return processed_titles

def hash_generator(processed_titles):
    hashs = []

    for i in range(len(processed_titles)):
        hashs.append(str(hashlib.md5(json.dumps(processed_titles[i], sort_keys=True).encode('utf-8')).hexdigest()))

    return hashs



if __name__ == '__main__':
    
    Google_ES_clean_logger()

    for data_path in glob.glob("/home/learn_compoundguru/Google_CNBC-Scraping/Project/ES_data/clean_google/*.csv"):
        logger_google_ES_clean.info("Started: %s" % ((data_path.split("/")[-1]).replace('.csv',"")))
        print(data_path)
        df = load_data(data_path)
        logger_google_ES_clean.info("Pass: Load Data")

        list_of_dicts = data_generator(df)
	
        processed_titles = processing_titles(list_of_dicts)
	
        hashs = hash_generator(processed_titles)
	

        logger_google_ES_clean.info("Pass: Processing Titles and Hashs")
	
        for i in range(len(processed_titles)):
            try:
                list_of_dicts[i]['title'] = processed_titles[i]
            except Exception as e:
                pass

        headers = {"Content-Type": "application/json"}
        data = {"sentences": processed_titles}

        response = requests.get("http://localhost:9602/predict", headers=headers, json=data)

        if response.status_code == 200:
            logger_google_ES_clean.info("Pass: Finbert Processing")
        else:
            logger_google_ES_clean.info("Failed: Finbert Processing")

        sentiment = []
        sentiment_score = []

        for i in range(len(json.loads(response.content))):
            sentiment.append((json.loads(response.content))[str(i)]['prediction'])
            sentiment_score.append((json.loads(response.content))[str(i)]['sentiment_score'])

        for i in range(len(list_of_dicts)):
            list_of_dicts[i]['sentiment'] = sentiment[i]
            list_of_dicts[i]['sentiment_score'] = sentiment_score[i]

        for i in range(len(list_of_dicts)):
            list_of_dicts[i]['search'] = processed_titles[i]

        
        for i in range(len(list_of_dicts)):
            try:
                for element in list_of_dicts[i]['tickers']:
                    value = (element.split(":")[1])
                    list_of_dicts[i]['tickers'].append(value)
            except Exception as e:
                pass
        
        for i in range(len(list_of_dicts)):
            try:
                value = abs(list_of_dicts[i]['sentiment_score'])
                list_of_dicts[i]['abs_sentiment_score'] = value
            except Exception as e:
                pass
        
        for i in range(len(list_of_dicts)):
            try:
                list_of_dicts[i]['authors'] = ((list_of_dicts[i]['authors'].lower()).strip())
                list_of_dicts[i]['title'] = ((list_of_dicts[i]['title']).strip())
            except Exception as e:
                pass


        new_items = []

        for i in range(len(list_of_dicts)):
            new_items.append({
                "_index": "news",
                "_id": hashs[i],
                "_op_type": "create",
                "_source": list_of_dicts[i]
            })

        with open(r'/home/learn_compoundguru/Google_CNBC-Scraping/Project/ES_data/Google_RSS/%s.json' % (data_path.split("/")[-1].replace('.csv',"")), 'w') as fout:
            json.dump(new_items, fout)

        logger_google_ES_clean.info("Completed: %s" % (data_path.split("/")[-1].replace('.csv',"")))
