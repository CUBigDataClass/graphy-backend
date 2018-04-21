import os
from glob import glob
import json
import re
import random
import nltk


os.environ['PYSPARK_PYTHON'] = '/Library/Frameworks/Python.framework/Versions/3.6/bin/python3.6'

from pyspark.sql import *
from pyspark import SparkContext
import string
import pandas as pd
from pyspark.sql.functions import col
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import NaiveBayes,LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF

## Extract actual necessary words from the tweet/reddit post (little pre-processing done here)
def extract_words(text_words):
    words = []
    # print(text_words)
    alpha_lower = string.ascii_lowercase
    alpha_upper = string.ascii_uppercase
    numbers = [str(n) for n in range(10)]
    text=" ".join(text_words)
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

## creating training data, using reddit and twitter data
def get_training_data():
    f_red= open('train_data_from_reddit.txt', 'r', encoding='utf-8')

    training_data = []
    f_s_p = open('twitter-topic-classifier-master//training.txt', 'r', encoding='utf-8')

    for l in f_s_p.readlines():
        l = l.strip()
        tweet_details = l.split()
        tweet_id = tweet_details[0]
        tweet_label = tweet_details[1].lower()
        # print(tweet_details[2:])
        tweet_words = extract_words(tweet_details[2:])
        training_data.append([tweet_label, tweet_words])

    f_s_p.close()

    for f_name in glob('tweets_news_events/*.json'):
        # print(f_name)
        with open(f_name) as json_data:
            d = json.load(json_data)
            l = f_name.split('/')[1]
            tweet_id = l.split('_')[1].split('.')[0]
            tweet_label = l.split('_')[0].lower()
            # print(d['text'].strip())
            tweet_words = extract_words(d['text'].strip().split())
            # print(tweet_words)
            training_data.append([tweet_label, tweet_words])


    for l in f_red.readlines():
        l = l.strip()
        reddit_details = l.split()
        reddit_label = reddit_details[0].lower()
        reddit_words = extract_words(reddit_details[1:])

        training_data.append([reddit_label, reddit_words])

    f_red.close()
    random.shuffle(training_data)
    return training_data


train_data=get_training_data()
print(train_data[0])

sc =SparkContext()
sqlContext = SQLContext(sc)
df = pd.DataFrame(train_data)
df.columns = ['text_label', 'text_words']
data=sqlContext.createDataFrame(df)
data.show(5)

## Counting no. of tweet/reddit post corresponding to each topic
data.groupBy("text_label") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

## regular expression tokenizer
regexTokenizer = RegexTokenizer(inputCol="text_words", outputCol="words", pattern="\\W")

## stop words
f=open("stopwords_twitter.txt","r")
add_stopwords =[]
for l in f.readlines():
    add_stopwords.append(l.strip())
print(add_stopwords)

stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered").setStopWords(add_stopwords)

## bag of words count
countVectors = CountVectorizer(inputCol="filtered", outputCol="features", binary=True, vocabSize=12000, minDF=1)

## creating the pipeline
label_stringIdx = StringIndexer(inputCol = "text_label", outputCol = "label")
pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, countVectors, label_stringIdx])

## creating the Naive Bayes classification model
# lr =  NaiveBayes(smoothing=1.0, modelType = "multinomial")

## creating the pipeline
# pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover,countVectors, label_stringIdx]+[lr])

# Fit the pipeline to training documents.
pipelineFit = pipeline.fit(data)
dataset = pipelineFit.transform(data)

trainingData=dataset
dataset.show(5)
#
# # set seed for reproducibility
# (trainingData, testData) = dataset.randomSplit([0.9, 0.1], seed = 100)
# print("Training Dataset Count: " + str(trainingData.count()))
# print("Test Dataset Count: " + str(testData.count()))

## creating the Naive Bayes classification model
lr =  NaiveBayes(smoothing=1.0, modelType = "multinomial",featuresCol="features",labelCol="label")
lrModel=lr.fit(trainingData)

# predictions = lrModel.transform(testData)
#
# predictions.filter(predictions['prediction'] == 0) \
#     .select("text_words","text_label","probability","label","prediction") \
#     .orderBy("probability", ascending=False) \
#     .show(n = 20, truncate = 30)
#
# evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
# print(evaluator.evaluate(predictions))

## Fit the pipeline to training documents.
# lrModel=pipeline.fit(trainingData)

## Save the model
lrModel.write().overwrite().save("NB_model_without_pipeline")

