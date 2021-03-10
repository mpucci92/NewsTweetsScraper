from const import DIR, parent
import os, sys
from socket import gaierror
import feedparser
import requests
from bs4 import BeautifulSoup
from pandas.io.json import json_normalize
import re
import numpy as np
import hashlib
import json
import glob as glob
import pandas as pd
import logging
from time import gmtime, strftime, localtime
import datetime as datetime
import Google_linux_merge_dev
from Google_linux_merge_dev import entire
import Google_to_Bucket
from Google_to_Bucket import Google_Bucket_entire
import ast

localtime = strftime("%Y-%m-%d %H-%M-%S", localtime())
time = datetime.date.today().strftime("%B %d, %Y")

ticker_name_list = []
percent = '%'

df = pd.read_csv(f'{parent}/data/tickers_yf.csv')
tickers = list(df.Ticker)
names = list(df.Name)

# Logger for google data #
def data_logger():
    global logger_google_data
    logger_google_data = logging.getLogger(__name__)
    logger_google_data.setLevel(logging.DEBUG)

    fh = logging.FileHandler(f'{parent}/logs/Google_logs.log')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger_google_data.addHandler(fh)

    logger_google_data.info('---START OF THE FILE---')
    logger_google_data.info('---GOOGLE DATA LOGS---')

# Function to get google TICKER data in CSV format #
def get_google_news(ticker):
    rss_url = f"https://news.google.com/rss/search?q={ticker}" + '+when:7d&hl=en-CA&gl=CA&ceid=CA:en'
    rss_url = rss_url.replace(" ","+")
    news_feed = feedparser.parse(rss_url)
    df_news_feed = json_normalize(news_feed.entries)
    data_path = f'{parent}/final_data/Pre_Google/'+ "Google_%s" % ticker + "_%s" % localtime + ".csv"

    if (len(df_news_feed)) != 0:
        df_news_feed.to_csv(data_path)  # input to save data

# Function to get google COMPANY NAME data in CSV format #
def get_google_news_name(name):
    rss_url = f"https://news.google.com/rss/search?q={name}" + '+when:7d&hl=en-CA&gl=CA&ceid=CA:en'
    rss_url = rss_url.replace(" ","+")
    news_feed = feedparser.parse(rss_url)
    df_news_feed_name = json_normalize(news_feed.entries)
    data_path = f'{parent}/final_data/Pre_Google/'+ "Google_%s" % name + "_%s" % localtime + ".csv"

    if (len(df_news_feed_name)) != 0:
        df_news_feed_name.to_csv(data_path)  # input to save data


# CREATE FINAL DATA FOLDER IF NOT THERE#
def init_folders():
    os.mkdir(f'{parent}/final_data')
    logger_google_data.info("FINAL DATA WAS CREATED")


if __name__ == '__main__':
    if os.path.isfile(f'{parent}/final_data'):
        init_folders()

    data_logger()

    for ticker in tickers:
        try:
            get_google_news(ticker)
            logger_google_data.info('%s:Completed', ticker)
            ticker_name_list.append(ticker)
            current_complete = (len(ticker_name_list) / len(tickers+names)) * 100
            logger_google_data.info('Current Percentage: %f %s', current_complete, percent)

        except Exception as e:
            logger_google_data.warning('Error Message: %s:%s', ticker, e)

    for name in names:
        try:
            get_google_news_name(name)
            logger_google_data.info('%s:Completed', name)
            ticker_name_list.append(name)
            current_complete = (len(ticker_name_list) / len(tickers+names)) * 100
            logger_google_data.info('Current Percentage: %f %s', current_complete, percent)

        except Exception as e:
            logger_google_data.warning('Error Message: %s:%s', name, e)

    percent_successful = (len(ticker_name_list) / len(tickers+names)) * 100
    logger_google_data.info('Percentage of successful tickers: %f  %s', percent_successful, percent)

    entire()

