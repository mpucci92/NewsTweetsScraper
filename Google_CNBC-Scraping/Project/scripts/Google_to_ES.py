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
def Google_ES_logger():
    global logger_google_ES
    logger_google_ES = logging.getLogger(__name__)
    logger_google_ES.setLevel(logging.DEBUG)

    fh = logging.FileHandler("/home/learn_compoundguru/Google_CNBC-Scraping/Project/ES_data/ES_logs/google_to_es_logs.log")
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger_google_ES.addHandler(fh)

    logger_google_ES.info('---START OF THE FILE---')
    logger_google_ES.info('---GOOGLE TO ES LOGS---')
    logger_google_ES.info("Start of News Document Count: %s " % (es_client.cat.count("rss", params={"format": "json"})[0]['count']))

if __name__ == '__main__':
    es_client = Elasticsearch(['35.203.120.124:8607'], http_compress=True,timeout=60)

    Google_ES_logger()

    # Script to input a clean Google dataset #
    for file in glob.glob("/home/learn_compoundguru/Google_CNBC-Scraping/Project/ES_data/Google_RSS/*.json"):
        
        with open(file) as json_file:
            data = json.load(json_file)
        
                
        try:
            
            successes, failures = helpers.bulk(es_client,data,index='news',stats_only=True,raise_on_error=False)

            #helpers.bulk(es_client,data,index='rss',raise_on_error=False,stats_only=True)
            logger_google_ES.info(f"Success: {successes}")
            logger_google_ES.info(f"Fails: {failures}")
            logger_google_ES.info(f"Successful bulk push to ES: {file}")
         
        except Exception as e:
            logger_google_ES.debug(f"Error with file: {file}")
 


