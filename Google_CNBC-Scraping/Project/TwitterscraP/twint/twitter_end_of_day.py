# Libraries #
import twint
import pandas as pd
from datetime import datetime,timedelta
import logging

# Variables #
date = (datetime.today().strftime('%Y-%m-%d'))
start_date = ((datetime.fromisoformat(date)) - timedelta(days=2)).strftime('%Y-%m-%d')
end_date = ((datetime.fromisoformat(date)) + timedelta(days=2)).strftime('%Y-%m-%d')

users = ['CNBC', 'Benzinga', 'Business', 'nytimesbusiness', 'ReutersMoney', 'ReutersBiz','stockhouse',
         'WSJmarkets', 'barronsonline', 'businessinsider', 'Forbes', 'MarketWatch', 'BusinessWire',
         'CNNBusiness', 'MorningstarInc', 'PRNewswire', 'YahooFinance', 'Deltaone', 'zerohedge','crypto', 
         'ForbesCrypto','BTCTN','CoinMarketCap','cointelegraph','crediblecrypto']

# Logger for merging data #
def Twitterscraper_logger():
    global logger_twitterscraper
    logger_twitterscraper = logging.getLogger(__name__)
    logger_twitterscraper.setLevel(logging.DEBUG)

    fh = logging.FileHandler(f"/home/learn_compoundguru/Google_CNBC-Scraping/Project/TwitterscraP/logs/twitter_raw_{date}.log")
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger_twitterscraper.addHandler(fh)

    logger_twitterscraper.info('--------------START OF LOG FILE------------------')
    logger_twitterscraper.info('-------------TWITTER SCRAPER LOGS----------------')
 

if __name__ == '__main__':

    data_path = f"/home/learn_compoundguru/Google_CNBC-Scraping/Project/TwitterscraP/data/twitter_raw/news_{start_date}_{end_date}.json"
    
    c = twint.Config()

    Twitterscraper_logger()
    
# counter for the log filrd
    counter = 0

    for name in users:
        c.Lang = 'en'
        c.Username = name
        c.Since = start_date
        c.Until = end_date
        c.Store_json = True
        c.Output = data_path   # Location to save raw twitter data
         
        try:
            twint.run.Search(c)
            logger_twitterscraper.info(f"Complete:{name}")
            counter = counter + 1
            logger_twitterscraper.info("Percentage completed:")
            logger_twitterscraper.info((counter/len(users)*100))
        except: 
            logger_twitterscraper.info(f"Error Processing Twitter Handle:{name}")
