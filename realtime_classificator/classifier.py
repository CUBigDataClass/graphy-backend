from sklearn.externals import joblib
import string
import re
import logging
import os
import socket
import json

from cassandra.cluster import Cluster
from cassandra.cqlengine import connection
from kafka import KafkaConsumer

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.INFO
)

KAFKA_SERVERS = os.environ.get('KAFKA_SERVERS', 'localhost')
IN_TREND_TOPIC = os.environ.get('IN_TREND_TOPIC', 'tweets')

TOPIC_C = joblib.load('topic_classifier_sklearn_updated.pkl')
SENTIMENT_C = joblib.load('sentiment_classifier_sklearn.pkl')


CASSANDRA_IPS = list(map(socket.gethostbyname, os.environ.get('CASSANDRA_NODES', '127.0.0.1').replace(' ', '').split(',')))

logging.info('Connecting to cassandra...')

cluster = Cluster(CASSANDRA_IPS)
session = cluster.connect()
prep_query = session.prepare("INSERT INTO graphy.tweet JSON ?")

def _store_dict(tweet):
    logging.debug('Output dict %s' % tweet)
    out_json = json.dumps(tweet, skipkeys=True)
    logging.debug('Output json %s' % out_json)
    session.execute(prep_query, [out_json, ])


def extract_words(text_words):
    words = []
    # print(text_words)
    alpha_lower = string.ascii_lowercase
    alpha_upper = string.ascii_uppercase
    numbers = [str(n) for n in range(10)]
    text = " ".join(text_words)
    p = re.sub(r'[^\w\s]', '', text)
    p = re.sub(" \d+", " ", p)
    # p=[i.lower() for i in p.split()]
    for word in p.split():
        cur_word = ''
        for c in word:
            if (c not in alpha_lower) and (c not in alpha_upper) and (c not in numbers):
                if len(cur_word) >= 2:
                    words.append(cur_word.lower())
                cur_word = ''
                continue
            cur_word += c
        if len(cur_word) >= 2:
            words.append(cur_word.lower())

    words_with_url = ' '.join(words)
    url_less_words = re.sub(
        r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))', '', words_with_url)
    return url_less_words


def prepare(body):
    return extract_words(body.strip().split())

def predict_topic(words):
    return TOPIC_C.predict([words,])
  
def predict_sentiment(words):
    return SENTIMENT_C.predict([words,])

if __name__ == '__main__':
    consumer = KafkaConsumer(IN_TREND_TOPIC, group_id='tweet_clasifier', bootstrap_servers=KAFKA_SERVERS, value_deserializer=lambda x: x.decode("utf-8"))
    with open('current_topic_categories.txt', 'r') as f:
        topics  = sorted(f.read().split())
        logging.info('Topic categories loaded: %s' % topics)
    with open('current_sentiment_categories.txt', 'r') as f:
        sentiments  = sorted(f.read().split())
        logging.info('Sentiment categories loaded: %s' % sentiments)
    for msg in consumer:
        if msg and msg.value:
            try:
                tweet = json.loads(msg.value)
            except Exception as e:
                logging.error('Message not processed %s' % msg.value)
                logging.error(e)
                continue
            body =  tweet.get('body', None)
            words =  prepare(body)
            if not body:
                logging.warn('No body for tweet. Skipping: %s' % msg.value)
                continue
            topic =  topics[predict_topic(words)[0]]
            sentiment =  sentiments[predict_sentiment(words)[0]]
            logging.info('Tweet received from Kafka. Topic: %s. Sentiment: %s' % (topic,sentiment))
            tweet['topic'] = topic
            tweet['sentiment'] = sentiment
            _store_dict(tweet)
            
