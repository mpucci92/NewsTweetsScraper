import hashlib
import json
import glob as glob
import pandas as pd
import numpy as np
import logging
from time import gmtime, strftime, localtime
import datetime as datetime
import os

# Local Time setting #
localtime = strftime("%Y-%m-%d %H-%M-%S", localtime())

# data to be merged #
data_to_be_merged = "/home/zqretrace/Google_CNBC-Scraping/Project/ES_data/clean_cnbc/*.csv"


# Merge File path #

merge_file_path = "/home/zqretrace/Google_CNBC-Scraping/Project/ES_data/es_merged_cnbc"

with open('/home/zqretrace/Google_CNBC-Scraping/Project/ES_data/hashlists/Google_hashlist.txt', 'r') as f:
    unique_hashs = f.read()

unique_hashs = unique_hashs.split("\n")

line_list = []


# Logger for merging data #
def CNBC_merge_data_logger():
    global logger_merge_cnbc_es
    logger_merge_cnbc_es = logging.getLogger(__name__)
    logger_merge_cnbc_es.setLevel(logging.DEBUG)

    fh = logging.FileHandler("/home/zqretrace/Google_CNBC-Scraping/Project/ES_data/ES_logs/CNBC_merge_es_logs.log")
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger_merge_cnbc_es.addHandler(fh)

    logger_merge_cnbc_es.info('---START OF THE FILE---')
    logger_merge_cnbc_es.info('---CNBC MERGE ES LOGS---')

# Merging CSV file function #
def merge_data(data_to_be_merged):
    global final_hashs

    for csvFile in glob.glob(data_to_be_merged):
        try:
            with open(csvFile, 'r', encoding="utf-8") as file:
                print("Started:%s" % csvFile)
                for line in file:
                    try:
                        line_split = line.split(',')

                        lines = line_split[0]

                        data_md5 = str(hashlib.md5(json.dumps(lines, sort_keys=True).encode('utf-8')).hexdigest())

                        if data_md5 not in unique_hashs:
                            unique_hashs.append(data_md5)
                            line_list.append(line)

                    except Exception as e:
                        logger_merge_cnbc_es.warning('Error Message: %s:%s', csvFile, e)
                        logger_merge_cnbc_es.warning('Failed to write line: %s', line)

        except Exception as e:
            logger_merge_cnbc_es.debug("%s_FAIL PRESENT" % csvFile)
        print("Completed:%s" % csvFile)

    final_hashs = list(set(unique_hashs))
    with open('/home/zqretrace/Google_CNBC-Scraping/Project/ES_data/hashlists/Google_hashlist.txt', 'w') as f:
        for item in unique_hashs:
            f.write("%s\n" % item)
    return (final_hashs)

# Writing the lines from list to a csv file #
def write_lines(merge_file_path):
    with open(merge_file_path + '/CNBC_Merged_%s' % localtime + '.csv', 'w') as Merged:
        for index,line in enumerate(line_list):
                try:
                    Merged.write(line)
                except Exception as e:
                    logger_merge_cnbc_es.info('%s FAILS PRESENT' % line)

def entire():
    CNBC_merge_data_logger()
    merge_data(data_to_be_merged)
    write_lines(merge_file_path)

if __name__ == '__main__':
    entire()
