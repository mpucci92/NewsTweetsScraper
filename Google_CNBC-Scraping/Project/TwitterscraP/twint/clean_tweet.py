from datetime import datetime
import time

def parse_time(source):
    date, _time, timezone = source['date'], source['time'], source['timezone']
    timezone = f"{timezone[:3]}:{timezone[3:]}"
    datestr = f"{date}T{_time}.000000{timezone}"
    timestamp = time.mktime(datetime.fromisoformat(datestr).utctimetuple())
    return datetime.fromtimestamp(timestamp).isoformat()

def clean_tweet(source):

    search = [[source['tweet']], source['hashtags'], source['cashtags']]
    search = [item for items in search for item in items]
    
    return {
        'title': source['tweet'],
        'published_parsed': parse_time(source),
        'acquisition_parsed': parse_time(source),
        'link': source['link'],
        'article_source': source['username'],
        'source': 'twitter',
        'search': search,
        'sentiment': source['sentiment'],
        'sentiment_score': source['sentiment_score'],
        'abs_sentiment_score': source['abs_sentiment_score']
    }
    
   