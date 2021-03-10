from const import DIR, parent
import logging
from time import gmtime, strftime, localtime
import datetime as datetime
import os
from const import DIR, parent
from google.cloud import storage
from CNBC_Merge_Local import delete_cnbc_data
import glob
import time
import tarfile as tar

localtime = strftime("%Y-%m-%d %H-%M-%S", localtime())
merged_source = 'Merged_CNBC'

compression_path = f'{parent}/final_data/' + "%s" % merged_source


directory_cnbc = os.listdir(compression_path)
slash = '/'

def cnbc_bucket_data_logger():
    global logger2
    logger2 = logging.getLogger(__name__)
    logger2.setLevel(logging.DEBUG)

    fh = logging.FileHandler(f'{parent}/logs/CNBC_merge_logs.log')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger2.addHandler(fh)

    logger2.info('---START OF THE FILE---')
    logger2.info('---CNBC BUCKET LOGS---')


def CNBC_upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)


def CNBC_bucket_entire():
    cnbc_bucket_data_logger()

    for file in directory_cnbc:
        try:
            CNBC_upload_blob("cnbc-storage",compression_path + '%s' % slash + "%s" % str(file), "CNBCNews/CNBC_Merged_%s.gz" % localtime)
            logger2.debug("Successful send %s" % file)
        except Exception as e:
            logger2.warning("Failed to write to the bucket: %s" % file)

    logger2.info('---START OF THE RSS FILES---')
    logger2.info('---RSS CNBC BUCKET LOGS---')
    
    for rss_file in glob.glob('/home/learn_compoundguru/Google_CNBC-Scraping/Project/ES_data/CNBC_RSS/*.json'):
        raw_tar = "/home/learn_compoundguru/Google_CNBC-Scraping/Project/ES_data/CNBC_RSS/CNBC_Merged_%s_CLEAN.tar.xz" % localtime
        with tar.open(raw_tar, mode="x:xz") as tar_file:
            tar_file.add(rss_file, arcname=os.path.basename(rss_file))

    for rss_file in glob.glob('/home/learn_compoundguru/Google_CNBC-Scraping/Project/ES_data/CNBC_RSS/*.tar.xz'):
        CNBC_upload_blob("cnbc-storage",rss_file, "Sentiment CNBC/CNBC_Merged_%s_CLEAN.tar.xz" % localtime)
        logger2.debug("Successful File: %s" % rss_file)

if __name__ == '__main__':
     CNBC_bucket_entire()
     delete_cnbc_data()
