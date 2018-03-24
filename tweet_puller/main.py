import json
import logging
import os
import socket

from cassandra.cluster import Cluster
from cassandra.cqlengine import connection
from kafka import KafkaConsumer
from searchtweets import load_credentials, gen_rule_payload, ResultStream

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.INFO
)

KAFKA_SERVERS = os.environ.get('KAFKA_SERVERS', 'localhost')
IN_TREND_TOPIC = os.environ.get('IN_TREND_TOPIC', 'new_trends')

enterprise_search_args = load_credentials()

# attributes to keep: text, user_id, timestamp, location, trend related to


CASSANDRA_IPS = list(
    map(socket.gethostbyname, os.environ.get('CASSANDRA_NODES', '127.0.0.1').replace(' ', '').split(',')))
KEYSPACE = 'graphy'

logging.info('Connecting to cassandra...')

cluster = Cluster(CASSANDRA_IPS)
with cluster.connect() as session:
    logging.info('Creating keyspace...')
    session.execute("""
           CREATE KEYSPACE IF NOT EXISTS %s
           WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
           """ % KEYSPACE)

    connection.setup(CASSANDRA_IPS, KEYSPACE, protocol_version=3)

session = connection.session
prep_query = session.prepare("INSERT INTO %s.tweet JSON ?" % KEYSPACE)


def _map_to_cassandra(tweet, trend):
    return {
        'text': tweet['text'],
        'trend': trend,
        'timestamp': tweet['created_at'],
        'location': None
    }


def _store_tweet(tweet, trend):
    session.execute_async(prep_query, [json.dumps(_map_to_cassandra(tweet, trend)), ])


def _download_tweets(trend):
    powertrack_rule = '(has:geo OR has:profile_geo) lang:en -is:retweet %s' % trend
    rule = gen_rule_payload(powertrack_rule, results_per_call=500)
    rs = ResultStream(rule_payload=rule, max_requests=2, **enterprise_search_args)
    for tweet in rs.stream():
        _store_tweet(tweet)


def main():
    consumer = KafkaConsumer(IN_TREND_TOPIC, group_id='tweet_puller', bootstrap_servers=KAFKA_SERVERS)
    for msg in consumer:
        if msg and msg.value:
            logging.info('Trend received from Kafka: %s' % msg.value)
            _download_tweets(msg.value)


if __name__ == '__main__':
    main()
