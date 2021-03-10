import pandas as pd
import regex as re
from datetime import datetime
import glob

def acquisition_time_format(df):
    for i in range(len(df)):
        try:
            df['acquisition time'].iloc[i] = (df['acquisition time']).iloc[i].split(" ")[0] + " " + (df['acquisition time']).iloc[i].split(" ")[1].replace("-",":")
        except Exception as e:
            pass

    return df


def ticker_finder(cnbc_df):
    title_tickers = {k: [] for k in range(len(cnbc_df))}

    for i in range(len(cnbc_df)):
            try:
                for name in re.findall('\((.*?)\)',str(cnbc_df.title.iloc[i])):

                    if (str(name).isupper() and len(str(name)) < 8):
                        title_tickers[i].append(name)
            except Exception as e:
                print("FAIL")

    pattern = "[A-Z]{3,7}-*[A-Z]{1,7}:[ ]{0,1}[A-Z]{1,7}-*[A-Z]{1,7}\.*[A-Z]{1,7}"

    for i in range(len(cnbc_df)):
            try:
                for name in re.findall(pattern,str(cnbc_df.title.iloc[i])):
                    if ((name not in title_tickers[i]) and (name.isupper())):
                        title_tickers[i].append(name)
            except Exception as e:
                print("FAIL")

    cnbc_df['Tickers'] = pd.Series(title_tickers)

    return cnbc_df

def ago_date(cnbc_df):
    flag_date = []

    for i in range(len(cnbc_df.date)):
        try:
            if ('ago' in cnbc_df.date.iloc[i]):

                    flag_date.append(1)
            
            elif ('Ago' in cnbc_df.date.iloc[i]):

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

    for i in range(len(cnbc_df.date)):

        if 'cnbc.com' in cnbc_df.link.iloc[i]:
            flag_publisher.append(2)

        elif 'seekingalpha.com' in cnbc_df.link.iloc[i]:
            flag_publisher.append(3)

        else:
            flag_publisher.append(4)

    cnbc_df['Publisher Flag'] = flag_publisher

    return cnbc_df

def replace_ago_date(cnbc_df):
    for i in range(len(cnbc_df)):
        if cnbc_df['Flag Date'].iloc[i] == 1:
            cnbc_df['date'].iloc[i] = cnbc_df['acquisition time'].iloc[i]
            
            
def replace_publisher_flag(cnbc_df):
    for i in range(len(cnbc_df)):
        
        if cnbc_df['Publisher Flag'].iloc[i] == 2:
            cnbc_df['Publisher Flag'].iloc[i] = 'CNBC'
        
        elif cnbc_df['Publisher Flag'].iloc[i] == 3:
            cnbc_df['Publisher Flag'].iloc[i] = 'Seeking Alpha'
        
        else:
            cnbc_df['Publisher Flag'].iloc[i] = 'Other'

            
def date_correcter(cnbc_df):
    cnbc_df.date = pd.to_datetime(cnbc_df.date)

def push_df_to_csv(dataframe,path):
    dataframe.to_csv(path,index=False)

if __name__ == '__main__':

    for files in glob.glob(r"/home/learn_compoundguru/Google_CNBC-Scraping/Project/final_data/Post_CNBC/*.csv"):

        print("Starting:%s" % files)

        cnbc_df = pd.read_csv(files,error_bad_lines=False)
        cnbc_df = cnbc_df.iloc[:,0:4]

        if len(cnbc_df.columns) == 3:
            cnbc_df.columns = ['title','date','link']
        if len(cnbc_df.columns) == 4:
            cnbc_df.columns = ['title','date','link','acquisition time']

        cnbc_df = acquisition_time_format(cnbc_df)

        col_number = len(cnbc_df.columns)

        cnbc_df = cnbc_df[~cnbc_df.date.str.contains("date")]
        name_of_file = files.split('/')[7].split(".")[0] + "_CLEAN.csv"

        ticker_finder(cnbc_df)
        ago_date(cnbc_df)
        source_finder_flag(cnbc_df)
        replace_ago_date(cnbc_df)
        replace_publisher_flag(cnbc_df)
       
        date_correcter(cnbc_df)
        cnbc_df.rename(columns={"date": "timestamp", "Publisher Flag": "authors", "Tickers":"tickers"},inplace=True)
        cnbc_df.drop(['Flag Date'],axis=1,inplace=True)

        print("Completed:%s" % files)
        push_df_to_csv(cnbc_df,f"/home/learn_compoundguru/Google_CNBC-Scraping/Project/ES_data/clean_cnbc/%s" % name_of_file)
