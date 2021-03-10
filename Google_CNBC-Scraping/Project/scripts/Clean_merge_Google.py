import hashlib
import json
import glob as glob
import pandas as pd
import numpy as np
import logging
from time import gmtime, strftime, localtime
import datetime as datetime
import os
import regex as re

# Local Time setting #
localtime = strftime("%Y-%m-%d %H-%M-%S", localtime())

slash = '/'

# load dictionary  - Specify the location of the Hashfile for Google and CNBC will be the same#
with open('/home/zqretrace/Google_CNBC-Scraping/Project/ES_data/hashlists/Google_hashlist.txt', "r") as data:
    unique_hashs = data.read()

unique_hashs = unique_hashs.split("\n")

# data to be merged #
data_to_be_merged = "/home/zqretrace/Google_CNBC-Scraping/Project/ES_data/clean_google/*.csv"

# Merge File path #

merge_file_path = "/home/zqretrace/Google_CNBC-Scraping/Project/ES_data/es_merged_google/"

with open('/home/zqretrace/Google_CNBC-Scraping/Project/ES_data/hashlists/Google_hashlist.txt', 'r') as f:
    unique_hashs = f.read()

unique_hashs = unique_hashs.split("\n")


line_list = []

# Logger for merging data #
def Google_merge_data_logger():
    global logger_merge
    logger_merge = logging.getLogger(__name__)
    logger_merge.setLevel(logging.DEBUG)

    fh = logging.FileHandler("/home/zqretrace/Google_CNBC-Scraping/Project/ES_data/ES_logs/clean_merge_es_logs.log")
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger_merge.addHandler(fh)

    logger_merge.info('---START OF THE FILE---')
    logger_merge.info('---GOOGLE ES MERGE LOGS---')

# Merging CSV file function #
def merge_data(data_to_be_merged):
    global final_hashs

    for csvFile in glob.glob(data_to_be_merged):
        try:
            with open(csvFile, 'r', encoding="utf-8") as file:
                print("Started:%s" % csvFile)
                for line in file:

                    try:
                        line_split = line.split(',')[0]

                        lines = re.sub(r'[^\x00-\x7F]+','', line_split)
                        lines = ''.join(char for char in lines if ord(char) < 128)

                        data_md5 = str(hashlib.md5(json.dumps(lines, sort_keys=True).encode('utf-8')).hexdigest())

                        if data_md5 not in set(unique_hashs):
                            unique_hashs.append(data_md5)
                            line_list.append(line)

                    except Exception as e:
                        logger_merge.warning('Error Message: %s:%s', csvFile, e)
                        logger_merge.warning('Failed to write line: %s', line)


        except Exception as e:
            logger_merge.debug("%s_FAIL PRESENT" % csvFile)
        print("Completed:%s" % csvFile)

    final_hashs = list(set(unique_hashs))
    with open('/home/zqretrace/Google_CNBC-Scraping/Project/ES_data/hashlists/Google_hashlist.txt', 'w') as f:
        for item in unique_hashs:
            f.write("%s\n" % item)

    return (final_hashs)

# Writing the lines from list to a csv file #
def write_lines(merge_file_path):
    with open(merge_file_path + '/Google_Merged_ES_%s' % localtime + '.csv', 'w') as Merged:
        for index,line in enumerate(line_list):
                try:
                    Merged.write(line)
                except Exception as e:
                    logger_merge.info('%s FAILS PRESENT' % line)

def entire():
    Google_merge_data_logger()
    merge_data(data_to_be_merged)
    write_lines(merge_file_path)

if __name__ == '__main__':
    entire()
