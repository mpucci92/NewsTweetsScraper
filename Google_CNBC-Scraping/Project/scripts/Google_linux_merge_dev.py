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

# Local Time setting #
localtime = strftime("%Y-%m-%d %H-%M-%S", localtime())

slash = '/'

# data source to merge #
source = 'Google'
pre_source = 'Pre_Google'
post_source = 'Post_Google'
merged_source = 'Merged_Google'


# load dictionary #
with open(f'{parent}/tmp/dictfile.txt', "r") as data:
    hash_dict = ast.literal_eval(data.read())

# data to be merged #
data_to_be_merged = f'{parent}/final_data/' + "%s" % pre_source + "/*.csv"

# Hash File location #
hash_path = f'{parent}/tmp/' + "dictfile.txt"

# Merge File path #

merge_file_path = f'{parent}/final_data/' + "%s" % post_source

# Compression Path #

compression_path = f'{parent}/final_data/' + "%s" % merged_source

dirs = os.listdir(compression_path)
unique_hashs = []
final_line_list = []
line_list = []
hash_entry = []

# Logger for merging data #
def Google_merge_data_logger():
    global logger_merge
    logger_merge = logging.getLogger(__name__)
    logger_merge.setLevel(logging.DEBUG)

    fh = logging.FileHandler(f'{parent}/logs/Google_merge_logs.log')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger_merge.addHandler(fh)

    logger_merge.info('---START OF THE FILE---')
    logger_merge.info('---GOOGLE MERGE LOGS---')


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

                        lines = line_split[1]
                        

                        data_md5 = str(hashlib.md5(json.dumps(lines, sort_keys=True).encode('utf-8')).hexdigest())

                        if data_md5 not in unique_hashs:
                            unique_hashs.append(data_md5)
                            line_list.append(line)

                    except Exception as e:
                        logger_merge.warning('Error Message: %s:%s', csvFile, e)
                        logger_merge.warning('Failed to write line: %s', line)

        except Exception as e:
            logger_merge.debug("%s_FAIL PRESENT" % csvFile)
    
    
    for index,text in enumerate(line_list):
        if unique_hashs[index] not in final_hashs:
            final_line_list.append(text)
            hash_entry.append(unique_hashs[index])

    del hash_dict[list(hash_dict)[0]]

    # create unique key at end of dictionary
    unique_key = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
    hash_dict[unique_key] = hash_entry

    with open(f'{parent}/tmp/dictfile.txt', 'w') as file:
        file.write(json.dumps(hash_dict))


# Writing the lines from list to a csv file #
def write_lines(merge_file_path):
    with open(merge_file_path + '/Google_Merged_%s' % localtime + '.csv', 'w') as Merged:
        for line in final_line_list:
            try:
                Merged.write(line)
            except Exception as e:
                logger_merge.info('%s FAILS PRESENT' % line)

# Function to compress files #
def tar_compression(compression_path):
    file_name = compression_path + "/Google_Merged_%s" % localtime + ".gz"

    tar = tarfile.open(file_name, "w:gz")

    os.chdir(merge_file_path)

    for name in os.listdir("."):
        try:
            tar.add(name)
            tar.close()
        except Exception as e:
            logger_merge.info('%s FAILS PRESENT' % name)

def delete_google_data():

    files = glob.glob(f'{parent}/final_data/' + "%s" % pre_source + "/*.csv")
    for f in files:
        os.remove(f)

    files = glob.glob(f'{parent}/final_data/' + "%s" % post_source+ "/*.csv")
    for f in files:
        os.remove(f)

    files = glob.glob(f'{parent}/final_data/' + "%s" % merged_source + "/*.gz")
    for f in files:
       os.remove(f)
  
    files = glob.glob(f'{parent}/ES_data/clean_google/*')
    for f in files:
       os.remove(f)

    files = glob.glob(f'{parent}/ES_data/Google_RSS/*')
    for f in files:
       os.remove(f)

def entire():
    Google_merge_data_logger()
    merge_data(data_to_be_merged)
    write_lines(merge_file_path)
    tar_compression(compression_path)
    
if __name__ == '__main__':
    entire()
