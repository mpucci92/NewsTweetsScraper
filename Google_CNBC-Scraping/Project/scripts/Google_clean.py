# Modules/Libraries #

import os
import json
import tarfile
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import regex as re
from dateutil.parser import parse
import glob
import json


df_tickers = pd.read_csv("/home/learn_compoundguru/Google_CNBC-Scraping/Project/data/clean_tickers.csv")
name_validation_flag = []
ticker_in_title_flag = []
pattern = "[A-Z]{3,7}-*[A-Z]{1,7}:[ ]{0,1}[A-Z]{1,7}-*[A-Z]{1,7}\.*[A-Z]{1,7}"
finance_source_present = []

l1 = "24/7 Wall Street,Barron's,Benzinga,Bloomberg Markets and Finance,Bloomberg,Bloomberg Markets,Bloomberg Technology,Business Insider,Business Wire,CNBC,CNET,CNN,CNN Business,Deadline,Engadget,ETF Trends,Fast Company, Forbes, Fox Business, GeekWire"
l2 = "Globe News Wire,GlobeNewsWire,GuruFocus,Huffington Post,Investopedia,Invezz,Investors Business Daily,Investor Place,Kiplinger,Market Watch,MarketWatch,Morningstar Inc.,Newsfile Corp,New York Post,New York Times,PR Newswire"
l3 = "Reuters,Reuters UK,Reuters India,Seeking Alpha,See It Market,Skynews,TechCrunch,The Motley Fool,The Street,Wall Street Journal,Yahoo Finance,Zacks Investment Research"

data_path ="/home/learn_compoundguru/Google_CNBC-Scraping/Project/final_data/Post_Google"

def google_dataframe(dataframe):
    df = dataframe
    df_goog = pd.DataFrame()
    df_goog['title'] = df.title
    df_goog['link'] = df.link
    df_goog['timestamp'] = df['published']
    df_goog['authors'] = df['source.title']
    df_goog['published_parsed'] = df['published_parsed']
    df_goog['source_href'] = df['source.href']
    df_goog['search_query'] = df['summary_detail.base']

    return df_goog

def new_query_array():
    array = []
    return array

def tickers(dataframe):
    query_tickers = []
    for i in range(len(dataframe)):
        try:
            qticker = re.sub(r'[^\w\s]','',dataframe.search_query.iloc[i])
            ticker_q = re.findall(r"([A-Z]+)",qticker)[0]
            query_tickers.append(ticker_q)
        except Exception as e:
            query_tickers.append(None)

    return query_tickers

def time_formatter(series):
    no_gmt = []
    for i in range(len(series)):
        try:
            val = series.iloc[i].replace('GMT','+0000')
            no_gmt.append(val)
        except Exception as e:
            no_gmt.append(None)
    return no_gmt

def date_parser(series):
    dateutil = []
    for i in range(len(series)):
        try:
            val = parse(series.iloc[i])
            dateutil.append(val)
        except Exception as e:
             dateutil.append(None)
    return dateutil

def company_list(string1,string2,string3):
    authors_list = []
    for item in string1.split(','):
        authors_list.append(item)

    for item in string2.split(','):
        authors_list.append(item)

    for item in string3.split(','):
        authors_list.append(item)

    names = list(set(authors_list))

    return names


def push_df_to_csv(dataframe,path):
    dataframe.to_csv(path,index=False)


for file in glob.glob("/home/learn_compoundguru/Google_CNBC-Scraping/Project/final_data/Post_Google/*.csv"):

    print(file)
    finance_source_present = []
    name_validation_flag = []
    ticker_in_title_flag = []

    df = pd.DataFrame()
    path = file
    name_of_file = path.split('/')[7].split(".")[0] + "_CLEAN.csv"


    df = pd.read_csv(file)
    df.columns = ['index', 'title', 'links', 'link', 'id', 'guidislink', 'published',
           'published_parsed', 'summary', 'title_detail.type',
           'title_detail.language', 'title_detail.base', 'title_detail.value',
           'summary_detail.type', 'summary_detail.language', 'summary_detail.base',
           'summary_detail.value', 'source.href', 'source.title']


    df = df.loc[:,['link', 'published', 'published_parsed', 'source.href', 'source.title','summary_detail.base', 'title']]
    df.columns


    df_google = google_dataframe(df)

    query_tickers = new_query_array()

    df_google['Ticker'] = tickers(df_google)

    df_google = df_google.merge(df_tickers, on='Ticker', how='left')

    df_google = df_google.drop_duplicates(subset=['title'])

    df_google.reset_index(drop=True,inplace=True)

    df_google = df_google.loc[:,['title', 'link', 'timestamp', 'authors', 'published_parsed',
    'source_href', 'search_query', 'Ticker', 'Name', 'ExchangeCode']]


    timestamps = time_formatter(df_google['timestamp'])
    df_google.drop(['timestamp'],axis=1,inplace=True)

    df_google['timestamp'] = timestamps

    df_google['timestamp'] = date_parser(df_google['timestamp'])
    times = pd.to_datetime(df_google['timestamp'],utc=True)
    df_google['timestamp'] = times

    for i in range(len(df_google)):
        try:
            if (df_google.Ticker.iloc[i] in df_google.title.iloc[i]) and (df_google.ExchangeCode.iloc[i] in df_google.title.iloc[i]) :
                ticker_in_title_flag.append(1)
            else:
                ticker_in_title_flag.append(0)
        except Exception as e:
            ticker_in_title_flag.append(0)

    df_google['ticker_in_title'] = ticker_in_title_flag

    for i in range(len(df_google)):
        try:
            if (df_google.Name.iloc[i] in df_google.title.iloc[i]):
                name_validation_flag.append(1)
            else:
                name_validation_flag.append(0)
        except Exception as e:
            name_validation_flag.append(0)

    df_google['name_in_title'] = name_validation_flag

    df_google = df_google.loc[:,['title', 'link', 'timestamp', 'authors','source_href', 'search_query', 'Ticker', 'Name', 'ExchangeCode','ticker_in_title', 'name_in_title']]

    df_google = df_google.sort_values(by=['timestamp'])
    df_google.reset_index(drop=True,inplace=True)
    df_google.rename({'Ticker':'ticker'},inplace=True)

    title_tickers = {k: [] for k in range(len(df_google))}

    for i in range(len(df_google)):
        for name in re.findall('\((.*?)\)',str(df_google.title.iloc[i])):
            if name.isupper():
                title_tickers[i].append(name)

    for i in range(len(df_google)):
        for name in re.findall(pattern,str(df_google.title.iloc[i])):
            if ((name not in title_tickers[i]) and (name.isupper())):
                title_tickers[i].append(name)

    df_google['ticker_tags'] = pd.Series(title_tickers)

    company_names = company_list(l1,l2,l3)

    for i in range(len(df_google)):
        if df_google.authors.iloc[i] in company_names:
            finance_source_present.append(1)
        else:
            finance_source_present.append(0)

    df_google['finance_source_present'] = finance_source_present

    df_google = df_google[(df_google['ticker_in_title'] == 1) | (df_google['name_in_title'] == 1) | (df_google['finance_source_present'] == 1)]

    df_google = df_google.drop_duplicates(subset='title')

    push_df_to_csv(df_google,"/home/learn_compoundguru/Google_CNBC-Scraping/Project/ES_data/clean_google/" + name_of_file )



