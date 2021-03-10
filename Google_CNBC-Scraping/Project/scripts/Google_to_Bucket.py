from const import DIR, parent
import logging
from time import gmtime, strftime, localtime
import datetime as datetime
import os
from google.cloud import storage
from Google_linux_merge_dev import delete_google_data
import glob as glob
import time
import tarfile as tar 

localtime = strftime("%Y-%m-%d_%H-%M-%S", localtime())
merged_source = 'Merged_Google'

compression_path = f'{parent}/final_data/' + "%s" % merged_source


directory = os.listdir(compression_path)
slash = '/'

def google_bucket_data_logger():
    global logger_bucket
    logger_bucket = logging.getLogger(__name__)
    logger_bucket.setLevel(logging.DEBUG)

    fh = logging.FileHandler(f'{parent}/logs/Google_merge_logs.log')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger_bucket.addHandler(fh)

    logger_bucket.info('---START OF THE FILE---')
    logger_bucket.info('---GOOGLE BUCKET LOGS---')


def Google_upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)


def Google_Bucket_entire():
    google_bucket_data_logger()

    for file in directory:
        Google_upload_blob("cnbc-storage",compression_path + '%s' % slash + "%s" % str(file), "GoogleNews/Google_Merged_%s.gz" % localtime)
        logger_bucket.debug("Successful File: %s" % file)
   
    logger_bucket.info('---START OF THE RSS FILES---')
    logger_bucket.info('---RSS GOOGLE BUCKET LOGS---')
    
    for rss_file in glob.glob('/home/learn_compoundguru/Google_CNBC-Scraping/Project/ES_data/Google_RSS/*.json'):
        raw_tar = "/home/learn_compoundguru/Google_CNBC-Scraping/Project/ES_data/Google_RSS/Google_Merged_%s_CLEAN.tar.xz" % localtime
        with tar.open(raw_tar, mode="x:xz") as tar_file:
            tar_file.add(rss_file, arcname=os.path.basename(rss_file))
    
    for rss_file in glob.glob('/home/learn_compoundguru/Google_CNBC-Scraping/Project/ES_data/Google_RSS/*.tar.xz'):
        Google_upload_blob("cnbc-storage",rss_file, "Sentiment GOOGLE/Google_Merged_%s_CLEAN.tar.xz" % localtime)
        logger_bucket.debug("Successful File: %s" % rss_file)
        time.sleep(2)    
   
    delete_google_data()


if __name__ == '__main__':
     Google_Bucket_entire()
      
