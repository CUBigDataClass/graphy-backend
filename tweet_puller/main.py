import json
import logging
import os
import socket
from datetime import datetime

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
KEYSPACE =  os.environ.get('IN_TREND_TOPIC', 'graphy')

enterprise_search_args = load_credentials(account_type='enterprise')

# attributes to keep: text, user_id, timestamp, location, trend related to


CASSANDRA_IPS = list(map(socket.gethostbyname, os.environ.get('CASSANDRA_NODES', '127.0.0.1').replace(' ', '').split(',')))

logging.info('Connecting to cassandra...')

cluster = Cluster(CASSANDRA_IPS)
session = cluster.connect()
prep_query = session.prepare("INSERT INTO graphy.tweet JSON ?")


def _map_to_cassandra(tweet, trend):

    try:
        location =  list(reversed(tweet['coordinates']['coordinates']))
    except (KeyError, TypeError):
        try:
            coords =  tweet['place']['bounding_box']['coordinates'][0]
            lat = sum([a[1] for a in coords])/4.0
            lon = sum([a[0] for a in coords])/4.0
            location = (lat,lon)
        except (KeyError, TypeError):
            try:
                location =  list(reversed(tweet['user']['derived']['locations'][0]['geo']['coordinates']))
            except (KeyError,TypeError) as e3:
                location =  None
                logging.error('Location can\'t be parsed')
                logging.error(e3)
    logging.info("Extracted location: %s" % json.dumps(location))
    logging.debug(json.dumps(tweet))
    return {
        'twid': tweet['id'],
        'body': str(tweet['text']),
        'trend': str(trend),
        'topic': None,
        'creation_time':  datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y').isoformat(),
        'user': str(tweet['user']['screen_name']),
        'location': ','.join(map(str,location)) if location else None
    }


def _store_tweet(tweet, trend):
    out = _map_to_cassandra(tweet, trend)
    logging.debug('Output dict %s' % out)
    out_json = json.dumps(out, skipkeys=True)
    logging.debug('Output json %s' % out_json)
    if out['location']:
        session.execute(prep_query, [out_json, ])


def _download_tweets(trend):
    powertrack_rule = '%s (has:geo OR has:profile_geo) lang:en -is:retweet' % trend
    rule = gen_rule_payload(powertrack_rule, results_per_call=500,to_date=None, from_date='201207220000')
    logging.info("PowerTrack rule: %s" % rule)
    rs = ResultStream(rule_payload=rule, max_results=500, max_requests=1, **enterprise_search_args)
    for tweet in rs.stream():
        _store_tweet(tweet, trend)


def main():
    consumer = KafkaConsumer(IN_TREND_TOPIC, group_id='tweet_puller', bootstrap_servers=KAFKA_SERVERS, value_deserializer=lambda x: x.decode("utf-8"))
    for msg in consumer:
        if msg and msg.value:
            logging.info('Trend received from Kafka: %s' % msg.value)
            _download_tweets(msg.value)


if __name__ == '__main__':
    main()
