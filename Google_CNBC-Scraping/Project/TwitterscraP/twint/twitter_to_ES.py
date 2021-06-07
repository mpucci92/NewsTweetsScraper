from elasticsearch import Elasticsearch
import numpy as np
import pandas as pd
from elasticsearch import helpers
import json
from datetime import datetime, timedelta
import glob
import logging


# Setting up the Elastic Search Connection #

# Logger for merging data #
def Twitter_ES_logger():
    global logger_twitter_ES
    logger_twitter_ES = logging.getLogger(__name__)
    logger_twitter_ES.setLevel(logging.DEBUG)

    fh = logging.FileHandler(r"/home/learn_compoundguru/Google_CNBC-Scraping/Project/TwitterscraP/logs/twitter_to_es_logs.log")
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger_twitter_ES.addHandler(fh)

    logger_twitter_ES.info('-------------------START OF THE FILE----------------------')
    logger_twitter_ES.info('----------TWITTER TO ES LOGS-----------')
    logger_twitter_ES.info("Start of News Document Count: %s " % (es_client.cat.count("tweets", params={"format": "json"})[0]['count']))


if __name__ == '__main__':
    es_client = Elasticsearch([''], http_compress=True, timeout=60)

    Twitter_ES_logger()

    # Script to input a clean Google dataset #
    for file in glob.glob(r"/home/learn_compoundguru/Google_CNBC-Scraping/Project/TwitterscraP/data/twitter_clean/*.json"):
        with open(file) as json_file:
            data = json.load(json_file)

        try:
            successes, failures = helpers.bulk(es_client, data, index='tweets', stats_only=True, raise_on_error=False)
            logger_twitter_ES.info(f"Successful Document Count: {successes}")
            logger_twitter_ES.info(f"Failed Document Count: {failures}")
            logger_twitter_ES.info("Successful bulk push to ES: %s " % (file))

        except Exception as e:
            logger_twitter_ES.debug("Error with file: %s" % (file))

        logger_twitter_ES.info("End of News Document Count: %s " % (es_client.cat.count("tweets", params={"format": "json"})[0]['count']))




