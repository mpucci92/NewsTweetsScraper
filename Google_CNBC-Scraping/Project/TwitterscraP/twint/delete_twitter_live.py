import os
import glob as glob

def delete_twitter_data():
    files = glob.glob("/home/learn_compoundguru/Google_CNBC-Scraping/Project/TwitterscraP/data/twitter_raw_live/*.json")
    for f in files:
        os.remove(f)

if __name__ == '__main__':
    delete_twitter_data()