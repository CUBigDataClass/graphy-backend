import json
import logging
import random
from collections import Counter, defaultdict
from trend_similarity import get_tweets_per_trend, \
    make_graph, vectorize_docs
import socket
from flask_cors import CORS
from flask import Flask, render_template, request, url_for, Response
from multiprocessing.dummy import Pool as ThreadPool
import os, json
from cassandra.cluster import Cluster
from cassandra.cqlengine import connection

PEOPLE_FOLDER = os.path.join('static', 'people_photo')

app = Flask(__name__, static_url_path='')

cors = CORS(app, resources={r"/*": {"origins": "*"}})
CASSANDRA_IPS = list(
    map(socket.gethostbyname, os.environ.get('CASSANDRA_NODES', '127.0.0.1').replace(' ', '').split(',')))


# This function returns a base geo json dictionary
def make_base_geojson():
    base_dict = {'features': [],
                 'properties': {'attribution': '',
                                'description': 'Tweet and trend analysis',

                                'fields': {'5055': {'name': 'Date'},
                                           '5056': {'name': 'Tweet'},
                                           '5057': {'name': 'Trend'},
                                           '5058': {'name': 'TweetID'},
                                           '5059': {'name': 'User'},
                                           '5065': {'lookup': {1: '🤬', # Anger
                                                               2: '😁', # Joy
                                                               3: '😐', #Neutral
                                                               4: '☹️'}, #Sadness
                                                    'name': 'Sentiment'},
                                           '5074': {'lookup': {1: 'Entertainment',
                                                               2: 'Mood',
                                                               3: 'Politics',
                                                               4: 'Sports',
                                                               5: 'Technology'},
                                                    'name': 'Topic'}}},
                 'type': 'FeatureCollection'}
    return base_dict


def geoJsonify(raw_json):
    topic_map = {'Entertainment': 1, 'Mood': 2, 'Politics': 3, 'Sports': 4, 'Technology': 5}
    sentiment_map = {'Anger': 1, 'Joy': 2, 'Neutral': 3, 'Sadness': 4}

    Date, Tweet, Topic = raw_json["creation_time"], raw_json["body"], raw_json["topic"]
    Trend, TweetID, User = raw_json["trend"], raw_json["twid"], raw_json["user"]
    Sentiment = raw_json["sentiment"]
    try:
        location = raw_json["location"].split(",")
    except:
        return None
    coordinates = [float(location[1]), float(location[0])]

    single_marker = {'geometry': {'coordinates': coordinates,
                                            'type': 'Point'},
                               'properties': {'5055': Date.split('.')[0], '5065': '4', '5074': '4'},
                               'type': 'Feature'}
    single_marker['properties']['5056'] = Tweet
    single_marker['properties']['5057'] = Trend
    single_marker['properties']['5058'] = TweetID
    single_marker['properties']['5059'] = User
    if not Topic:
        Topic = random.choice(['Entertainment', 'Mood', 'Politics', 'Sports', 'Technology'])
    single_marker['properties']['5074'] = topic_map[Topic]
    if not Sentiment:
        Sentiment = random.choice(['Anger', 'Joy', 'Neutral', 'Sadness'])
    single_marker['properties']['5065'] = sentiment_map[Sentiment]
    return single_marker


@app.route('/all_markers', methods=['GET','POST'])
def show_all_markers():
    # full_filename = os.path.join(app.config['UPLOAD_FOLDER'], 'shovon.jpg')
    inverse_topic_map = {1: 'Entertainment', 2: 'Mood',
                         3: 'Politics', 4: 'Sports', 5: 'Technology'}
    inverse_sentiment_map = {1: 'Anger', 2: 'Joy', 3: 'Neutral', 4: 'Sadness'}

    cluster = Cluster(CASSANDRA_IPS)
    session = cluster.connect('graphy')
    rows = session.execute("SELECT json * FROM tweet limit 100000")
    result = []
    topic_counts, sentiment_counts = Counter(), Counter()
    trend_class_counter, trend_sentiment_counter = defaultdict(Counter), defaultdict(Counter)
    trend_classification, trend_sentiment = {}, {}


    for i in rows:
        raw_json = json.loads(i.json)

        geo_json = geoJsonify(raw_json)
        if geo_json:
            result.append(geo_json)

            #Counting topics for each trend
            topic = inverse_topic_map[geo_json['properties']['5074']]
            topic_counts[topic] += 1
            trend_class_counter[geo_json['properties']['5057']][topic] += 1

            #Counting sentiment for each trend
            sentiment = inverse_sentiment_map[geo_json['properties']['5065']]
            sentiment_counts[sentiment] += 1
            trend_sentiment_counter[geo_json['properties']['5057']][sentiment] += 1

    #Picking the topic for a trend based on majority topic
    for trend in trend_class_counter:
        trend_classification[trend] = trend_class_counter[trend].most_common()[0][0]

    # Picking the sentiment for a trend based on majority sentiment
    for trend in trend_sentiment_counter:
        trend_sentiment[trend] = trend_sentiment_counter[trend].most_common()[0][0]

    base_geo_json = make_base_geojson()
    base_geo_json['features'] = result
    return Response(json.dumps({'geo_json' : base_geo_json,
                                'topic_counts' : topic_counts,
                                'trend_classification' : trend_classification,
                                'sentiment_counts' : sentiment_counts,
                                'trend_sentiment' : trend_sentiment}),
                    mimetype='application/json')


@app.route('/trend_graph', methods=['GET'])
def get_trend_graph():
    trend_doc = get_tweets_per_trend(n_trends=50)

    result = vectorize_docs(trend_doc)

    graph = make_graph(result)

    return Response(json.dumps(graph),
                    mimetype='application/json')


if __name__ == '__main__':
   app.run()