import hashlib
import numpy as np
import pandas as pd
import json
import requests
import glob
import re

def load_data(data_path):
    df = pd.read_csv(data_path)
    
    if len(df.columns) == 6:

        df = df.loc[:, ['title', 'timestamp', 'link', 'acquisition time', 'tickers','authors']]

        df.columns = ['title', 'timestamp', 'link', 'acquisition time', 'tickers','authors']

    df.timestamp = pd.to_datetime(df['timestamp'], utc=True)

    df.timestamp = df['timestamp'].dt.tz_localize(None)

    iso_dates = []

    for i in range(len(df)):
        iso_dates.append(df.timestamp.iloc[i].isoformat())

    df.timestamp = iso_dates

    try:
        df.drop(['acquisition time'], axis=1, inplace=True)
    except Exception as e:
        pass

    return df


def None_filler(df,columns):
    for col in columns:
        df.loc[df[col].isnull(), [col]] = df.loc[df[col].isnull(), col].apply(lambda x: [])
    return df


def data_generator(df):
    list_dict = []

    for index, row in list(df.iterrows()):
        list_dict.append(dict(row))

    for i in range(len(list_dict)):
        try:
            list_dict[i]['tickers'] = eval(list_dict[i]['tickers'])
        except Exception as e:
            pass

    list_of_dicts = []

    for i in range(len(list_dict)):
        res = {key: value for key, value in list_dict[i].items() if ((len(value) > 0))}
        list_of_dicts.append(res)

    return list_of_dicts


def processing_titles(list_of_dicts):
    titles = []
    for i in range(len(list_of_dicts)):
        try:
            titles.append(list_of_dicts[i]['title'])
        except Exception as e:
            titles.append("NO TITLE")

    return titles


def hash_generator(processed_titles):
    hashs = []

    for i in range(len(processed_titles)):
        hashs.append(str(hashlib.md5(json.dumps(processed_titles[i], sort_keys=True).encode('utf-8')).hexdigest()))

    return hashs

if __name__ == '__main__':
    for data_path in glob.glob('/home/learn_compoundguru/Google_CNBC-Scraping/Project/ES_data/clean_cnbc/*.csv'):

        df = load_data(data_path)

        df = None_filler(df, df.columns)

        list_of_dicts = data_generator(df)

        processed_titles = processing_titles(list_of_dicts)

        hashs = hash_generator(processed_titles)

        for i in range(len(processed_titles)):
            try:
                list_of_dicts[i]['title'] = processed_titles[i]
            except Exception as e:
                pass

        headers = {"Content-Type": "application/json"}
        data = {"sentences": processed_titles}

        response = requests.get("http://localhost:9602/predict", headers=headers, json=data)

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
                value = abs(list_of_dicts[i]['sentiment_score'])
                list_of_dicts[i]['abs_sentiment_score'] = value
            except Exception as e:
                pass
        
        for i in range(len(list_of_dicts)):
            try:
                list_of_dicts[i]['authors'] = ((list_of_dicts[i]['authors'].lower()).strip())
                if (list_of_dicts[i]['authors'] == 'cnbc.com'):
                    list_of_dicts[i]['authors'] = 'cnbc'
                list_of_dicts[i]['title'] = ((list_of_dicts[i]['title']).strip())
            except Exception as e:
                pass
        
        ticker_in_title_flag = []
        pattern = "[A-Z]{3,7}-*[A-Z]{1,7}:[ ]{0,1}[A-Z]{1,7}-*[A-Z]{1,7}\.*[A-Z]{1,7}"

        title_tickers = {k: [] for k in range(len(list_of_dicts))}

        for i in range(len(list_of_dicts)):
            for name in re.findall('\((.*?)\)',list_of_dicts[i]['title']):
                if name.isupper():
                    title_tickers[i].append(name)

        for i in range(len(list_of_dicts)):
            for name in re.findall(pattern,list_of_dicts[i]['title']):
                if ((name not in title_tickers[i]) and (name.isupper())):
                    title_tickers[i].append(name)

        for i in range(len(list_of_dicts)):
            for name in re.findall(r"([A-Z]+)",list_of_dicts[i]['title']):
                if ((name not in title_tickers[i]) and (name.isupper())):
                    if len(name) > 2:
                        title_tickers[i].append(name)

        tickers_complete = pd.Series(title_tickers)

        for i in range(len(list_of_dicts)):
            if (len(tickers_complete[i]) > 0):
                list_of_dicts[i]['tickers'] = tickers_complete[i]

        for i in range(len(list_of_dicts)):
            try:
                for element in list_of_dicts[i]['tickers']:
                    value = (element.split(":")[1])
                    list_of_dicts[i]['tickers'].append(value)
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

        with open(r'/home/learn_compoundguru/Google_CNBC-Scraping/Project/ES_data/CNBC_RSS/%s.json' % (data_path.split("/")[-1].replace('.csv', "")),
                  'w') as fout:
            json.dump(new_items, fout)

