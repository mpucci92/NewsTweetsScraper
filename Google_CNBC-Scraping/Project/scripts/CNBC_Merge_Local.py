# Single Day Script - Aggregate all of these at the end #
import hashlib
import json
import glob as glob 
import pandas as pd
import numpy as np
import logging
from time import gmtime, strftime, localtime
import datetime as datetime
import os
import tarfile
from const import DIR, parent
import random, string
from google.cloud import storage
import ast


# Local Time setting
localtime = strftime("%Y-%m-%d %H-%M-%S", localtime())

# data source to merge #
source = 'CNBC'
pre_source = 'Pre_CNBC'
post_source = 'Post_CNBC'
merged_source = 'Merged_CNBC'

# load dictionary #
with open(f'{parent}/tmp/CNBC_dictfile.txt', "r") as data:
    hash_dict = ast.literal_eval(data.read())

# data to be merged #
data_to_be_merged = f'{parent}/final_data/' + "%s" % pre_source +  "/*.csv"



# Hash File location #
hash_path = f'{parent}/tmp/CNBC_dictfile.txt'

# Merge File path #

merge_file_path = f'{parent}/final_data/' + "%s" % post_source

# Compression Path #
compression_path = f'{parent}/final_data/' + "%s" % merged_source

dirs = os.listdir(compression_path)
slash = '/'

unique_hashs = []

line_list = []

final_line_list = []

hash_entry = []

# Logger for merging data #
def cnbc_merge_data_logger():
    global logger1
    logger1 = logging.getLogger(__name__)
    logger1.setLevel(logging.DEBUG)

    fh = logging.FileHandler(f'{parent}/logs/CNBC_merge_logs.log')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger1.addHandler(fh)

    logger1.info('---START OF THE FILE---')
    logger1.info('---CNBC MERGE LOGS---')


# Merging CSV file function #
def merge_data(data_to_be_merged):
    empty = []

    for value in hash_dict.values():
        empty = empty + value


    final_hashs = list(set(empty))

    for csvFile in glob.glob(data_to_be_merged):
        try:
            with open(csvFile, 'r', encoding="utf-8") as file:
                 
                 for line in file:
                    try:
                        line_split = line.split(',')

                        lines = line_split[0]

                        data_md5 = str(hashlib.md5(json.dumps(lines, sort_keys=True).encode('utf-8')).hexdigest())

                        if data_md5 not in unique_hashs:
                            unique_hashs.append(data_md5)
                            line_list.append(line)

                    except Exception as e:
                        logger1.warning('Error Message: %s:%s', csvFile, e)
                        logger1.warning('Failed to write line: %s', line)

        except Exception as e:
            logger1.debug("%s_FAIL PRESENT" % csvFile)
    
    
    for index,text in enumerate(line_list):
        if unique_hashs[index] not in final_hashs:
            final_line_list.append(text)
            hash_entry.append(unique_hashs[index])

    del hash_dict[list(hash_dict)[0]]

    # create unique key at end of dictionary
    unique_key = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
    hash_dict[unique_key] = hash_entry

    with open(f'{parent}/tmp/CNBC_dictfile.txt', 'w') as file:
         file.write(json.dumps(hash_dict))


# Writing the lines from list to a csv file #
def write_lines(merge_file_path):
    with open(merge_file_path + '/CNBC_Merged_%s' % localtime + '.csv', 'w') as Merged:
        for line in final_line_list:
             try:
                 Merged.write(line)
                 logger1.info("Success for line %s" % line)
             except Exception as e:
                 logger1.info('%s FAILS PRESENT' % line)

def tar_compression(compression_path):
    file_name = compression_path + "/CNBC_Merged_%s" % localtime + ".gz"

    tar = tarfile.open(file_name, "w:gz")

    os.chdir(merge_file_path)

    for name in os.listdir("."):
        try:
            tar.add(name)
            tar.close()
            logger1.info("Success for file %s" % name)
        except Exception as e:
            logger1.info('%s FAILS PRESENT' % name)


def delete_cnbc_data():

    files = glob.glob(f'{parent}/final_data/' + "%s" % pre_source + "/*.csv")
    for f in files:
        os.remove(f)

    files = glob.glob(f'{parent}/final_data/' + "%s" % post_source + "/*.csv")
    for f in files:
        os.remove(f)

    files = glob.glob(f'{parent}/final_data/' + "%s" % merged_source + "/*.gz")
    for f in files:
        os.remove(f)

    files = glob.glob(f'{parent}/ES_data/clean_cnbc/*')
    for f in files:
        os.remove(f)

    files = glob.glob(f'{parent}/ES_data/CNBC_RSS/*')
    for f in files:
        os.remove(f) 

def CNBC_entire():
    cnbc_merge_data_logger()
    merge_data(data_to_be_merged)
    write_lines(merge_file_path)
    tar_compression(compression_path)


if __name__ == '__main__':
    CNBC_entire()
