import json
import os
import time
from pprint import pprint

import tweepy
from searchtweets import load_credentials, gen_rule_payload, ResultStream
from twitter_public_api_key import *

def _chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def _generate_api_obj():
    API_KEY = consumer_key
    API_SECRET = consumer_secret

    auth = tweepy.AppAuthHandler(API_KEY, API_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    if (not api):
        print ("Can't Authenticate!!")
        return None
    return api

def _store_tweet(tweet):
    pass

def _download_tweets(trend, enterprise_search_args):
    powertrack_rule = '(has:geo OR has:profile_geo) lang:en -is:retweet %s' % trend
    rule = gen_rule_payload(powertrack_rule, results_per_call=500)
    rs = ResultStream(rule_payload=rule, max_requests=2, **enterprise_search_args)
    for tweet in rs.stream():
        print(tweet)
        _store_tweet(tweet)

def _id_classification_lookup(file):
    id_classification_lookup = {}
    with open(file, 'r') as f:
        ids_classifications = f.read().strip().split('\n')
        for id_classification in ids_classifications:
            id, classification = tuple(id_classification.split())
            id_classification_lookup[id] = classification
    return id_classification_lookup

def _ids_to_tweets_downloader(ids, api, folder, id_classification_lookup):
    '''
    :param id: tweet ids (supports max 100 ids in one go)
    :param folder: location for downloading tweet
    :return: None (downloads tweets as json)
    '''
    tweets = api.statuses_lookup(ids)

    for i, tweet in enumerate(tweets):
        tweet_dict = tweet._json
        # tweet_dict = json.loads(tweet_str)
        id = tweet_dict['id_str']
        with open(os.path.join(folder, id_classification_lookup[id] +\
                                       '_' + id + '.json'), 'w') as outfile:
            json.dump(tweet_dict, outfile)


if __name__ == "__main__":
    # enterprise_search_args = load_credentials(filename="./.twitter_keys.yaml")
    # _download_tweets("throwbackthursday", enterprise_search_args)

    api = _generate_api_obj()

    id_classification_lookup = _id_classification_lookup('labeled_tweet_ids_news_event.txt')

    ids = list(id_classification_lookup.keys())
    ids_meta_list = list(_chunks(ids, 100))

    for i, ids_chunk in enumerate(ids_meta_list):
        try:
            _ids_to_tweets_downloader(ids_chunk, api, 'tweets_news_events/', id_classification_lookup)
        except tweepy.RateLimitError:
            print("\nWarning : Rate limit crossed.\n")
            time.sleep(16 * 60)
            _ids_to_tweets_downloader(ids_chunk, api, 'tweets_news_events/', id_classification_lookup)
        print("{} done.".format((i+1)*100))

