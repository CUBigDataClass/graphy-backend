import json
import os
import socket
import random
import string
import gensim
from collections import defaultdict
from pprint import pprint

from cassandra.cluster import Cluster

CASSANDRA_IPS = list(
    map(socket.gethostbyname, os.environ.get('CASSANDRA_NODES', '127.0.0.1').replace(' ', '').split(',')))


def get_tweets_per_trend(n_trends = 100):
    cluster = Cluster(CASSANDRA_IPS)
    session = cluster.connect('graphy')
    rows = session.execute("SELECT json * FROM tweet limit 100000")
    shuffled_rows = []

    for i in rows:
        raw_json = json.loads(i.json)
        shuffled_rows.append({'trend':raw_json['trend'],
                              'tweet':raw_json['body']})
    random.shuffle(shuffled_rows)
    trend_doc = defaultdict(list)

    for row in shuffled_rows:
        if len(trend_doc) == n_trends:
            continue
        if len(trend_doc[row['trend']]) > 10:
            continue
        trend_doc[row['trend']].append(row['tweet'])

    exclude = set(string.punctuation)
    for trend in trend_doc:
        s = " ".join(trend_doc[trend])
        s = s.strip().lower()
        s = ''.join(" " if ch == "-" else ch for ch in s)
        s = ''.join(ch for ch in s if ch not in exclude)
        s = ''.join([i for i in s if not i.isdigit()])
        trend_doc[trend] = s

    return trend_doc

def vectorize_docs(trend_doc):
    sentences = []
    for trend in trend_doc:
        sentences.append(gensim.models.doc2vec.TaggedDocument(
            words=trend_doc[trend].split(), tags=[trend]))

    model = gensim.models.doc2vec.Doc2Vec(alpha=0.025, min_alpha=0.025)
    model.build_vocab(sentences)
    model.train(sentences, total_examples = model.corpus_count, epochs=1)

    result = {}
    for trend in trend_doc:
        # print("*****************************")
        # print(trend)
        # print(" = ")
        # print(model.docvecs.most_similar(trend))
        result[trend] = model.docvecs.most_similar(trend)
    model.delete_temporary_training_data(keep_doctags_vectors=True, keep_inference=True)
    return result

def make_graph(result):
    graph = {"nodes" : [], "links" : []}
    trends = sorted(result.keys())
    trend_index = {}
    for i, trend in enumerate(trends):
        graph["nodes"].append({"name" : trend})
        trend_index[trend] = i

    for trend in trends:
        for each_edge in result[trend]:
            distance = 2 - each_edge[1]
            if distance < 2:
                edge = {"source":trend_index[trend],
                        "target":trend_index[each_edge[0]],
                        "value":3**distance}
                graph["links"].append(edge.copy())

    return graph

if __name__ == '__main__':
    trend_doc = get_tweets_per_trend(n_trends = 100)

    result = vectorize_docs(trend_doc)

    graph = make_graph(result)

    pprint(graph)