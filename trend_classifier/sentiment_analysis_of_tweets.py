
import re
import random
import os

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
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.evaluation import MulticlassMetrics


## Extract actual necessary words from the tweets (little pre-processing done here)
def extract_words(text_words):
    words = []
    # print(text_words)
    alpha_lower = string.ascii_lowercase
    alpha_upper = string.ascii_uppercase
    numbers = [str(n) for n in range(10)]
    text=text_words
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

## creating training data, using twitter data lablled for 4 emotions ('anger','joy','neutral','sadness')
def get_training_data():
    f= open('/Users/saumya/Desktop/Big_data_project/tweet_emotion_labelled_data.txt', 'r', encoding='utf-8')

    training_data = []

    for l in f.readlines():
        l = l.strip()
        tweet_details = l.split('\t')
        # print(tweet_details)
        tweet_id = tweet_details[0]
        tweet_words = extract_words(tweet_details[1])
        # print(tweet_details[2:])
        emotion_label=tweet_details[2]
        if emotion_label=='fear':
            emotion_label='neutral'
        emotion_score=tweet_details[3]
        training_data.append([emotion_label,tweet_words])

    f.close()

    random.shuffle(training_data)
    return training_data


train_data=get_training_data()

## creating a spark context
sc =SparkContext()
sqlContext = SQLContext(sc)

df = pd.DataFrame(train_data)
df.columns = ['text_label','text_words', ]
data=sqlContext.createDataFrame(df)
data.show(5)

## Counting no. of tweets corresponding to each sentiment
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
countVectors = CountVectorizer(inputCol="filtered", outputCol="features", binary=True, vocabSize=10000, minDF=1)

## creating the pipeline
label_stringIdx = StringIndexer(inputCol = "text_label", outputCol = "label")
pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover,countVectors, label_stringIdx])


# Fit the pipeline to training documents.
pipelineFit = pipeline.fit(data)
dataset = pipelineFit.transform(data)

trainingData=dataset
# dataset.show(5)
#
##set seed for reproducibility
# (trainingData, testData) = dataset.randomSplit([0.9, 0.1], seed = 100)
# print("Training Dataset Count: " + str(trainingData.count()))
# print("Test Dataset Count: " + str(testData.count()))

# creating the Naive Bayes classification model
lr =  NaiveBayes(smoothing=1.0, modelType = "multinomial")
lrModel=lr.fit(trainingData)

# predictions = lrModel.transform(testData)
#
# predictions.filter(predictions['prediction'] == 0) \
#     .select("text_words","text_label","probability","label","prediction") \
#     .orderBy("probability", ascending=False) \
#     .show(n = 20, truncate = 30)
#
# evaluator = MulticlassClassificationEvaluator(labelCol="label",predictionCol="prediction")
# print(evaluator.evaluate(predictions))


# saving the model
lrModel.write().overwrite().save("Sentiment_model")