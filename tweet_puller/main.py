import json
import logging
import os
import socket
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer
from searchtweets import load_credentials, gen_rule_payload, ResultStream

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.INFO
)

KAFKA_SERVERS = os.environ.get('KAFKA_SERVERS', 'localhost')
IN_TREND_TOPIC = os.environ.get('IN_TOPIC', 'new_trends')
OUT_TOPIC = os.environ.get('OUT_TOPIC', 'tweets')
PRODUCER = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    retries=5,
    value_serializer=lambda v: v.encode('utf-8')
)

enterprise_search_args = load_credentials(account_type='enterprise')

# attributes to keep: text, user_id, timestamp, location, trend related to


def _map_to_dict(tweet, trend):

    try:
        location = list(reversed(tweet['coordinates']['coordinates']))
    except (KeyError, TypeError):
        try:
            coords = tweet['place']['bounding_box']['coordinates'][0]
            lat = sum([a[1] for a in coords])/4.0
            lon = sum([a[0] for a in coords])/4.0
            location = (lat, lon)
        except (KeyError, TypeError):
            try:
                location = list(
                    reversed(tweet['user']['derived']['locations'][0]['geo']['coordinates']))
            except (KeyError, TypeError) as e3:
                location = None
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
        'location': ','.join(map(str, location)) if location else None
    }


def _push_tweet(tweet, trend):
    out = _map_to_dict(tweet, trend)
    logging.debug('Output dict %s' % out)
    out_json = json.dumps(out, skipkeys=True)
    logging.debug('Output json %s' % out_json)
    if out['location']:
        PRODUCER.send(OUT_TOPIC, out_json)
        logging.info('Sent!')


def _download_tweets(trend):
    powertrack_rule = '%s (has:geo OR has:profile_geo) lang:en -is:retweet' % trend
    rule = gen_rule_payload(
        powertrack_rule, results_per_call=500, to_date=None, from_date='201207220000')
    logging.info("PowerTrack rule: %s" % rule)
    rs = ResultStream(rule_payload=rule, max_results=500,
                      max_requests=1, **enterprise_search_args)
    for tweet in rs.stream():
        _push_tweet(tweet, trend)


def main():
    consumer = KafkaConsumer(IN_TREND_TOPIC, group_id='downloader',
                             bootstrap_servers=KAFKA_SERVERS, value_deserializer=lambda x: x.decode("utf-8"))
    for msg in consumer:
        if msg and msg.value:
            logging.info('Trend received from Kafka: %s' % msg.value)
            _download_tweets(msg.value)


if __name__ == '__main__':
    main()
