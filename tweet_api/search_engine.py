'''
Author : Shivendra Agrawal
'''

from datetime import datetime
from pprint import pprint
from elasticsearch import Elasticsearch
from urllib.request import urlopen
import csv

#
# FILE_URL = "https://github.com/dsindy/kaggle-titanic/blob/master/data/test.csv"
#
# ES_HOST = {"host" : "localhost", "port" : 9200}
#
# INDEX_NAME = 'titanic'
# TYPE_NAME = 'passenger'
#
# ID_FIELD = 'passengerid'
#
#
# response = urlopen(FILE_URL)
# csv_file_object = csv.reader(response, 'r')
#
# header = next(csv_file_object)
# header = [item.lower() for item in header]
#
# print(header)
#
# bulk_data = []
#
# for row in csv_file_object:
#     data_dict = {}
#     for i in range(len(row)):
#         data_dict[header[i]] = row[i]
#     op_dict = {
#         "index": {
#         	"_index": INDEX_NAME,
#         	"_type": TYPE_NAME,
#         	"_id": data_dict[ID_FIELD]
#         }
#     }
#     bulk_data.append(op_dict)
#     bulk_data.append(data_dict)
# # pprint(bulk_data)
# print(len(bulk_data))
#
#
# from elasticsearch import Elasticsearch
#
# # create ES client, create index
# es = Elasticsearch(hosts = [ES_HOST])
#
# if es.indices.exists(INDEX_NAME):
#     print("deleting '%s' index..." % (INDEX_NAME))
#     res = es.indices.delete(index = INDEX_NAME)
#     print(" response: '%s'" % (res))
#
# # since we are running locally, use one shard and no replicas
# request_body = {
#     "settings" : {
#         "number_of_shards": 1,
#         "number_of_replicas": 0
#     }
# }
#
# print("creating '%s' index..." % (INDEX_NAME))
# res = es.indices.create(index = INDEX_NAME, body = request_body)
# print(" response: '%s'" % (res))
#
# # bulk index the data
# print("bulk indexing...")
# res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)

es = Elasticsearch()
es.index(index="my-index", doc_type="test-type", id=42, body={"any": "data", "timestamp": datetime.now()})
pprint(es.get(index="my-index", doc_type="test-type", id=42)['_source'])