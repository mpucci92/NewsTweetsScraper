from const import DIR, parent, date_today
from bs4 import BeautifulSoup
import requests
import sys, os
import hashlib
import json 
import pandas as pd
import numpy as np
import logging
from time import gmtime, strftime, localtime
import datetime as datetime
import CNBC_Merge_Local 
from CNBC_Merge_Local import CNBC_entire 

# Local Time setting
localtime = strftime("%Y-%m-%d %H-%M-%S", localtime())

URL = "https://www.cnbc.com/quotes/?symbol={ticker}&qsearchterm={ticker}&tab=news"
path = '/home/learn_compoundguru/Google_CNBC-Scraping/Project/data/tickers_yf.csv' 
df = pd.read_csv(path)
TICKERS = list(df.Ticker)
ticker_list = []
percent = '%'

source = 'CNBC'
pre_source = 'Pre_CNBC'
post_source = 'Post_CNBC'
merged_source = 'Merged_CNBC'


def data_logger():
    global logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(f'{parent}/logs/CNBC_logs.log')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.info('---START OF THE FILE---')
    logger.info('---CNBC DATA LOGS---')
    return logger

def get_news(ticker):
    url = URL.format(ticker=ticker)
    page = requests.get(url).content
    page = BeautifulSoup(page)

    titles = []

    for i in range(len((page.findAll("div", {"class": "LatestNews-headline"})))):
        titles.append(page.findAll("div", {"class": "LatestNews-headline"})[i].text)

    timestamp = []
   
    # Code to extract the timestamps #
    for i in range(len((page.findAll("div", {"class": "LatestNews-timestamp"})))):
        timestamp.append(page.findAll("div", {"class": "LatestNews-timestamp"})[i].text)
    
    links = []

    for element in page.findAll("div", {"class": "LatestNews-headline"}):
        for a in element.find_all('a', href=True): 
            if a.text: 
                links.append(a['href'])
    
    acquisition_time = []
    
    for i in range(len((page.findAll("div", {"class": "LatestNews-timestamp"})))):
        acquisition_time.append(localtime)

    df = pd.DataFrame()
    
    df['title'] = titles
    df['date'] = timestamp
    df['link'] = links
    df['acquisition time'] = acquisition_time

    if len(df) != 0:
        df.to_csv("/home/learn_compoundguru/Google_CNBC-Scraping/Project/final_data/Pre_CNBC/" + "%s" % ticker + "_%s" % date_today+".csv", sep=',', index=False)


if __name__ == '__main__':

    data_logger()
    
    for ticker in TICKERS:

        try:
            get_news(ticker)
            logger.info('%s:Completed', ticker)
            ticker_list.append(ticker)
            current_complete = (len(ticker_list) / len(TICKERS)) * 100
            logger.info('Current Percentage: %f %s', current_complete, percent)

        except Exception as e:
            logger.warning('Error Message: %s:%s', ticker, e)
            continue

    percent_successful = (len(ticker_list) / len(TICKERS)) * 100
    logger.info('Percentage of successful tickers: %f  %s', percent_successful, percent)
    
    
    CNBC_entire()
