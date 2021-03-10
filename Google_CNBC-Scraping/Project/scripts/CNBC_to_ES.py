from elasticsearch import Elasticsearch
import numpy as np
import pandas as pd
from elasticsearch import helpers
import json
from datetime import datetime, timedelta
import glob
import logging

# Setting up the Elastic Search Connection #  # EPHERMAL PORTS USED EACH TIME VMs ARE CLOSED#

# Logger for merging data #
def CNBC_ES_logger():
    global logger_CNBC_ES
    logger_CNBC_ES = logging.getLogger(__name__)
    logger_CNBC_ES.setLevel(logging.DEBUG)

    fh = logging.FileHandler("/home/zqretrace/Google_CNBC-Scraping/Project/ES_data/ES_logs/cnbc_to_es_logs.log")
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger_CNBC_ES.addHandler(fh)

    logger_CNBC_ES.info('---START OF THE FILE---')
    logger_CNBC_ES.info('---CNBC TO ES LOGS---')
    logger_CNBC_ES.info("Start of News Document Count: %s " % (es_client.cat.count("news", params={"format": "json"})[0]['count']))

if __name__ == '__main__':
    es_client = Elasticsearch(['34.95.46.239:8607'], http_compress=True,timeout=60)

    CNBC_ES_logger()

    # Script to input a clean Google dataset #
    for file in glob.glob("/home/zqretrace/Google_CNBC-Scraping/Project/ES_data/es_merged_cnbc/*.csv"):

        df = pd.read_csv(file,encoding ="latin-1")

        df.columns = ['title','link','acquisition time','timestamp','authors','article_type','tickers']
        df = df.loc[:,['title', 'link', 'timestamp', 'authors', 'article_type', 'tickers']]
        df = df.dropna()
        df.timestamp = pd.to_datetime(df.timestamp)

        timestamp_iso = []
        for i in range(len(df)):
            timestamp_iso.append(pd.to_datetime(df.timestamp.iloc[i]).isoformat())

        df.tickers = df.tickers.replace("[]","None")


        df.timestamp = timestamp_iso
        data = df.to_json(orient='records') # CSV - DF - JSON
        data = json.loads(data)
        

        try:
            helpers.bulk(es_client,data,index='news')
            logger_CNBC_ES.info("Successful bulk push to ES: %s " % (file))
         
        except Exception as e:
            logger_CNBC_ES.debug("Error with file: %s" % (file))
   
         
        logger_CNBC_ES.info("End of News Document Count: %s " % (es_client.cat.count("news", params={"format":"json"})[0]['count']))
