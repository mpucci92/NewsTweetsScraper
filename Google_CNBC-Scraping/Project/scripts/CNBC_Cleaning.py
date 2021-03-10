import os
import json
import tarfile
import pandas as pd
import numpy as np
from urllib.parse import urlparse
import regex as re
from datetime import datetime
import glob

def cnbc_create_dataframe(cnbc_df):
    dates = []
    publisher = []
    type_ = []

    for i in range(len(cnbc_df)):
        try:
            dates.append(cnbc_df.date.iloc[i].split('-')[0])
        except Exception as e:
            dates.append(None)

        try:
            publisher.append(cnbc_df.date.iloc[i].split('-')[1])
        except Exception as e:
            publisher.append(None)

        try:
            type_.append(cnbc_df.date.iloc[i].split('-')[2])
        except Exception as e:
            type_.append(None)

    cnbc_df['Date'] = dates
    cnbc_df['Publisher'] = publisher
    cnbc_df['Article_Type'] = type_

    cnbc_df.drop(['date'],axis=1,inplace=True)

    return cnbc_df


def ticker_finder(cnbc_df):
    title_tickers = {k: [] for k in range(len(cnbc_df))}

    for i in range(len(cnbc_df)):
            try:
                for name in re.findall('\((.*?)\)',cnbc_df.title.iloc[i]):

                    if (str(name).isupper() and len(str(name)) < 8):
                        title_tickers[i].append(name)
            except Exception as e:
                print("FAIL")

    pattern = "[A-Z]{3,7}-*[A-Z]{1,7}:[ ]{0,1}[A-Z]{1,7}-*[A-Z]{1,7}\.*[A-Z]{1,7}"

    for i in range(len(cnbc_df)):
            try:
                for name in re.findall(pattern,cnbc_df.title.iloc[i]):
                    if ((name not in title_tickers[i]) and (name.isupper())):
                        title_tickers[i].append(name)
            except Exception as e:
                print("FAIL")

    cnbc_df['Tickers'] = pd.Series(title_tickers)

    return cnbc_df

def ago_date(cnbc_df):
    flag_date = []

    for i in range(len(cnbc_df.Date)):
        try:
            if 'ago' in cnbc_df.Date.iloc[i]:

                    flag_date.append(1)

            else:
                flag_date.append(0)
        except Exception as e:
            flag_date.append(None)

    cnbc_df['Flag Date'] = flag_date

    return cnbc_df

def source_finder_flag(cnbc_df):
    # 2 CNBC
    # 3 Seeking aplha
    # 4 Other

    flag_publisher = []

    for i in range(len(cnbc_df.Date)):

        if 'cnbc.com' in cnbc_df.link.iloc[i]:
            flag_publisher.append(2)

        elif 'seekingalpha.com' in cnbc_df.link.iloc[i]:
            flag_publisher.append(3)

        else:
            flag_publisher.append(4)

    cnbc_df['Publisher Flag'] = flag_publisher

    return cnbc_df

def missing_dates(cnbc_df):
    cnbc_date = {k: [] for k in range(len(cnbc_df))}

    for i in range(len(cnbc_df)):

        if (('cnbc.com' in cnbc_df.link.iloc[i]) and ('video' in cnbc_df.link.iloc[i])):
            x = cnbc_df.link.iloc[i]
            string_date = '-'.join([x.split('/')[6],x.split('/')[5],x.split('/')[4]])

            if (bool(re.search('[a-zA-Z]', string_date))) == False:
                cnbc_date[i].append(string_date)

        elif 'cnbc.com' in cnbc_df.link.iloc[i]:
            x = cnbc_df.link.iloc[i]
            string_date = '-'.join([x.split('/')[5],x.split('/')[4],x.split('/')[3]])

            if (bool(re.search('[a-zA-Z]', string_date))) == False:
                cnbc_date[i].append(string_date)

        else:
            cnbc_date[i].append(None)


    cnbc_df['missing dates'] = pd.Series(cnbc_date)


    for i in range(len(cnbc_df)):
        if ((cnbc_df['Flag Date'].iloc[i] == 1) and (cnbc_df['Publisher Flag'].iloc[i] == 2)):
            cnbc_df['Date'].iloc[i] = cnbc_df['missing dates'].iloc[i]

    cnbc_df.drop(['Flag Date','Publisher Flag','missing dates'],axis=1,inplace=True)

    return cnbc_df


def push_df_to_csv(dataframe,path):
    dataframe.to_csv(path,index=False)

def month_to_num(date):
    calendar_dict = {'Jan': "01",'Feb': "02", 'Mar': "03", 'Apr': "04", 'May': "05", 'Jun': "06", 'Jul': "07", 'Aug': "08", 'Sep': "09",'Oct': "10", 'Nov': "11", 'Dec': "12"}
    new_date = date.replace(date.split(' ')[1],calendar_dict[date.split(' ')[1]])
    new_date = new_date.rstrip()
    return new_date.replace(" ","-")

def new_date(cnbc_df):
    new_dates = []
    for i in range(len(cnbc_df)):
        try:
            new_dates.append(month_to_num(cnbc_df.Date.iloc[i]))
        except Exception as e:
            new_dates.append(cnbc_df.Date.iloc[i])

    clean_dates = []

    for element in new_dates:

        if type(element) == list:
            try:
                clean_dates.append(element[0])
            except Exception as e:
                clean_dates.append(None)

        else:
            clean_dates.append(element)

    cnbc_df['Date'] = clean_dates

    return cnbc_df

def format_time(cnbc_df):
    formatted_time = []

    for i in range(len(cnbc_df)):
        date_string = cnbc_df.timestamp.iloc[i]
        try:
            date_object = datetime.strptime(date_string, "%d-%m-%Y")
            date_object = date_object.strftime("%Y-%m-%d %H:%M:%S")
            formatted_time.append(date_object)
        except Exception as e:
            date_string = "01-01-9999"
            date_object = datetime.strptime(date_string, "%d-%m-%Y")
            date_object = date_object.strftime("%Y-%m-%d %H:%M:%S")
            formatted_time.append(date_object)

    cnbc_df['timestamp'] = formatted_time
    cnbc_df = cnbc_df.sort_values(by=['timestamp'],ignore_index=True)
    print("PERCENTAGE OF VALUES MISSING TIME")
    print(100*(len(cnbc_df[cnbc_df['timestamp'] == '01-01-1970 00:00:00']) / len(cnbc_df)))
    return cnbc_df

if __name__ == '__main__':

    for files in glob.glob("/home/zqretrace/Google_CNBC-Scraping/Project/final_data/Post_CNBC/*.csv"):

        print("Starting:%s" % files)

        cnbc_df = pd.read_csv(files,error_bad_lines=False)
        cnbc_df = cnbc_df.iloc[:,0:4]

        if len(cnbc_df.columns) == 3:
            cnbc_df.columns = ['title','date','link']
        if len(cnbc_df.columns) == 4:
            cnbc_df.columns = ['title','date','link','acquisition time']

        col_number = len(cnbc_df.columns)

        cnbc_df = cnbc_df.drop_duplicates(subset=['title'])

        cnbc_df = cnbc_df[~cnbc_df.date.str.contains("date")]

        name_of_file = files.split('/')[5].split(".")[0] + "_CLEAN.csv"

        cnbc_create_dataframe(cnbc_df)


        ticker_finder(cnbc_df)
        ago_date(cnbc_df)
        source_finder_flag(cnbc_df)
        missing_dates(cnbc_df)
        new_date(cnbc_df)
        cnbc_df.fillna("None",inplace=True)
        cnbc_df.rename(columns={"Date": "timestamp", "Publisher": "authors", "Tickers":"tickers","Article_Type":"article_type"},inplace=True)
        format_time(cnbc_df)

        cnbc_df = cnbc_df.replace("9999-01-01 00:00:00", "1970-01-01 00:00:00")

        for i in range(len(cnbc_df)):
            if (cnbc_df.timestamp.iloc[i] == "1970-01-01 00:00:00") and (col_number == 4):
                cnbc_df.timestamp.iloc[i] = cnbc_df['acquisition time'].iloc[i]

        cnbc_df.timestamp = pd.to_datetime(cnbc_df.timestamp)
        if col_number == 4:
            cnbc_df['acquisition time'] = pd.to_datetime(cnbc_df['acquisition time'])

        print("Completed:%s" % files)
        push_df_to_csv(cnbc_df,"/home/zqretrace/Google_CNBC-Scraping/Project/ES_data/clean_cnbc/%s" % name_of_file)
