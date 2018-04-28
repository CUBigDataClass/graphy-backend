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
                 'properties': {'attribution': 'Traffic accidents: <a '
                                               'href="http://data.norge.no/data/nasjonal-vegdatabank-api" '
                                               'target="blank">NVDB</a>',
                                'description': 'Tweet and trend analysis',

                                'fields': {'5055': {'name': 'Date'},
                                           '5056': {'name': 'Tweet'},
                                           '5057': {'name': 'Trend'},
                                           '5058': {'name': 'TweetID'},
                                           '5059': {'name': 'User'},
                                           '5065': {'Sentiment': {'1': 'Pedestrian',
                                                               '2': 'Bicycle',
                                                               '3': 'Motorcycle',
                                                               '4': 'Car'},
                                                    'name': 'Sentiment'},
                                           '5074': {'lookup': {'1': 'Event',
                                                               '2': 'Sports',
                                                               '3': 'Politics',
                                                               '4': 'News',
                                                               '5': 'Technology',
                                                               '6': 'Business',
                                                               '7': 'Entertainment',
                                                               '8': 'Health'},
                                                    'name': 'Topic'}}},
                 'type': 'FeatureCollection'}
    return base_dict


def geoJsonify(raw_json):
    topic_map = {'Business': '6', 'Entertainment': '7', 'Event': '1', 'Health': '8',
                'News': '4', 'Politics': '3', 'Sports': '2', 'Technology': '5'}

    Date, Tweet, Topic = raw_json["creation_time"], raw_json["body"], raw_json["topic"]
    Trend, TweetID, User = raw_json["trend"], raw_json["twid"], raw_json["user"]
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
        Topic = random.choice(['Entertainment', 'Business', 'Health', 'News', 'Sports',
                               'Event', 'Politics', 'Technology'])
    single_marker['properties']['5074'] = topic_map[Topic]
    return single_marker


@app.route('/all_markers', methods=['GET','POST'])
def show_all_markers():
    # full_filename = os.path.join(app.config['UPLOAD_FOLDER'], 'shovon.jpg')
    inverse_topic_map = {'1': 'Event', '2': 'Sports', '3': 'Politics',
                        '4': 'News', '5': 'Technology', '6': 'Business',
                        '7': 'Entertainment', '8': 'Health'}
    cluster = Cluster(CASSANDRA_IPS)
    session = cluster.connect('graphy')
    rows = session.execute("SELECT json * FROM tweet limit 100000")
    result = []
    topic_counts = Counter()
    trend_class_counter = defaultdict(Counter)
    trend_classification = {}


    for i in rows:
        raw_json = json.loads(i.json)

        geo_json = geoJsonify(raw_json)
        if geo_json:
            result.append(geo_json)
            topic = inverse_topic_map[geo_json['properties']['5074']]
            topic_counts[topic] += 1
            trend_class_counter[geo_json['properties']['5057']][topic] += 1
    for trend in trend_class_counter:
        trend_classification[trend] = trend_class_counter[trend].most_common()[0][0]

    base_geo_json = make_base_geojson()
    base_geo_json['features'] = result
    return Response(json.dumps({'geo_json' : base_geo_json,
                                'topic_counts' : topic_counts,
                                'trend_classification' : trend_classification}),
                    mimetype='application/json')

@app.route('/trend_graph', methods=['GET'])
def get_trend_graph():
    trend_doc = get_tweets_per_trend(n_trends=100)

    result = vectorize_docs(trend_doc)

    graph = make_graph(result)

    return Response(json.dumps(graph),
                    mimetype='application/json')

@app.route('/color', methods=['POST'])
def show_color():
    color = request.form['color']
    return "Success"


if __name__ == '__main__':
   app.run()