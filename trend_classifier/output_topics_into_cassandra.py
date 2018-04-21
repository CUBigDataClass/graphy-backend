#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from pyspark import SparkContext,SparkConf
import string
import pandas as pd
from pyspark.ml import Pipeline,PipelineModel
import random
import re
from pyspark.ml.classification import NaiveBayesModel
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.functions import col
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler,IndexToString
from pyspark.ml.feature import HashingTF, IDF

# make sure pyspark tells workers to use python3 not 2 if both are installed
# os.environ['PYSPARK_PYTHON'] = '/Library/Frameworks/Python.framework/Versions/3.6/bin/python3.6'
# os.environ["PYSPARK_DRIVER_PYTHON"]='/Library/Frameworks/Python.framework/Versions/3.6/bin/python3.6'

spark = SparkSession.builder \
  .appName('SparkCassandraApp') \
  .config('spark.cassandra.connection.host', 'localhost') \
  .config('spark.cassandra.connection.port', '9042') \
  .config('spark.cassandra.output.consistency.level','ONE') \
  .config('spark.cassandra.output.consistency.level','ONE') \
  .master('local[2]') \
  .getOrCreate()


sqlContext = SQLContext(spark)



data = (
  sqlContext
    .read
    .format('org.apache.spark.sql.cassandra')
    .options(table='tweet', keyspace='graphy')
    .load()
)

data.show(10)
print(data.count())
# twid_for_none_tweets=data.where(col("topic").isNull()).select(col('twid'))
data_filtered = data.where(col("topic").isNull()).select('trend','creation_time','twid','body')
data_filtered.show(5)
print(data_filtered.count())

# Extract actual necessary words from the reddit
def extract_words(text_words):
    words = []
    alpha_lower = string.ascii_lowercase
    alpha_upper = string.ascii_uppercase
    numbers = [str(n) for n in range(10)]
    text=text_words
    # print('hi')
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
    url_less_words = re.sub(r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))','', words_with_url)
    return url_less_words


extract_words_udf=udf(extract_words,StringType())
data_filtered=data_filtered.withColumn('text_words', extract_words_udf('body'))
data_filtered.show()

regexTokenizer = RegexTokenizer(inputCol="text_words", outputCol="words", pattern="\\W")

## stop words
f=open("/Users/saumya/Desktop/Big_data_project/stopwords_twitter.txt","r")
add_stopwords =[]
for l in f.readlines():
    add_stopwords.append(l.strip())
print(add_stopwords[:5])

stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered").setStopWords(add_stopwords)

## bag of words count
countVectors = CountVectorizer(inputCol="filtered", outputCol="features", binary=True, vocabSize=12000, minDF=1)


# label_stringIdx = StringIndexer(inputCol = "text_label", outputCol = "label")
pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, countVectors])

## creating the Naive Bayes classification model
# lr =  NaiveBayes(smoothing=1.0, modelType = "multinomial")

## creating the pipeline
# pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover,countVectors, label_stringIdx]+[lr])

# Fit the pipeline to training documents.

df = data_filtered.select('trend','creation_time',"twid","text_words")
pipelineFit = pipeline.fit(df)
dataset = pipelineFit.transform(df)
dataset.show(5)
dataset.count()

model = NaiveBayesModel.load('/Users/saumya/Desktop/Big_data_project/NB_model_without_pipeline')
print(model)

predictions = model.transform(dataset)

labeler = IndexToString(inputCol="prediction", outputCol="predictedLabel",labels=['event','sports','politics','news','technology','business','entertainment','health'])
# print(predictions)
prediciton_with_label=labeler.transform(predictions)
prediciton_with_label.show(5)
print(prediciton_with_label.count())

ta = data.alias('ta')
tb = prediciton_with_label.select('trend','creation_time','twid','predictedLabel').alias('tb')

final_df=ta.join(tb,(ta.twid==tb.twid) & (ta.creation_time==tb.creation_time) & (ta.trend==tb.trend),how="left").select(ta.trend,ta.creation_time,ta.twid,ta.body,ta.location,ta.topic,ta.user,tb.predictedLabel)
final_df.show()
print(final_df.count())
# final_df=final_df.drop('twid')

final_df=final_df.withColumn('topic',coalesce(final_df.topic,final_df.predictedLabel))

final_df=final_df.drop('predictedLabel')
print(final_df.count())
final_df.show(5)
# final_df.limit(100).write.csv('/Users/saumya/Desktop/Big_data_project/topic_classification.csv')
final_df.write.mode('append').format('org.apache.spark.sql.cassandra').options(table = 'tweet', keyspace = 'graphy').save()