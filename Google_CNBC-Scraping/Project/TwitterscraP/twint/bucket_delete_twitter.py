import logging
from time import gmtime, strftime, localtime
import datetime as datetime
import os
from google.cloud import storage
import glob as glob

localtime = strftime("%Y-%m-%d %H-%M-%S", localtime())

compression_path = f'/home/learn_compoundguru/Google_CNBC-Scraping/Project/TwitterscraP/data/twitter_raw'
slash = '/'
clean_compression_path = f'/home/learn_compoundguru/Google_CNBC-Scraping/Project/TwitterscraP/data/twitter_clean'

raw_directory = os.listdir(compression_path)
clean_directory = os.listdir(clean_compression_path)

def twitter_bucket_data_logger():
    global logger2
    logger2 = logging.getLogger(__name__)
    logger2.setLevel(logging.DEBUG)

    fh = logging.FileHandler(f'/home/learn_compoundguru/Google_CNBC-Scraping/Project/TwitterscraP/logs/twitter_bucket_logs.log')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger2.addHandler(fh)

    logger2.info('---------------START OF THE FILE----------------')
    logger2.info('---------TWITTER BUCKET LOGS-------------')


def twitter_upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)


def twitter_bucket_entire():
    twitter_bucket_data_logger()

    for file in raw_directory:
        try:
            twitter_upload_blob("cnbc-storage",compression_path + '%s' % slash + "%s" % str(file), "TwitterNews/Twitter_Raw_%s.tar" % localtime)
            logger2.debug("Successful send to TwitterNews: %s" % file)

        except Exception as e:
            logger2.warning("Failed to write to the bucket TwitterNews: %s" % file)


    for file in clean_directory:
        try:
            twitter_upload_blob("cnbc-storage",clean_compression_path + '%s' % slash + "%s" % str(file), "Sentiment TWITTER/Twitter_Clean_%s.tar" % localtime)
            logger2.debug("Successful send to Sentiment TWITTER: %s" % file)

        except Exception as e:
            logger2.warning("Failed to write to the bucket Sentiment TWITTER: %s" % file)


def delete_twitter_data():
    files = glob.glob(compression_path + "/*.json")
    for f in files:
        os.remove(f)

    files = glob.glob(clean_compression_path + "/*.json")
    for f in files:
        os.remove(f)

if __name__ == '__main__':
     twitter_bucket_entire()
     delete_twitter_data()
