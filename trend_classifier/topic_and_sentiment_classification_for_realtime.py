import re
import random
import string
import pandas as pd
from glob import glob
import json

import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn import preprocessing
from sklearn.feature_extraction import text
from sklearn.metrics import precision_recall_fscore_support as score
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, classification_report, confusion_matrix
import pickle
from sklearn.externals import joblib

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


df = pd.DataFrame(train_data)
df.columns = ['text_label','text_words']
X=df['text_words'].values
y=df['text_label'].values

print(type(X))
print(X[:10])
print(y[:10])

## split X and y numpy arrays to train & test
# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
# print (X_train.shape, y_train.shape)
# print (X_test.shape, y_test.shape)

## encoding text labels
le = preprocessing.LabelEncoder()
Y = le.fit_transform(y)
print(Y.shape)
## stop words
f=open("stopwords_twitter.txt","r")
add_stopwords =[]
for l in f.readlines():
    add_stopwords.append(l.strip())
print(add_stopwords)

stopwords = text.ENGLISH_STOP_WORDS.union(add_stopwords)

##Creating pipeline with countvectorizer and classifier
classifier = Pipeline([
    ('vectorizer', CountVectorizer(ngram_range=(1,2),stop_words=stopwords,min_df=1,max_df=10000,binary=True)),
    ('clf', MultinomialNB(alpha=1.0))])

classifier.fit(X, Y)
# predicted = classifier.predict(X_test)
# all_labels = le.inverse_transform(predicted)
# y_test_labels=le.fit_transform(y_test)
#
# print(accuracy_score(y_test_labels, predicted))
# print(precision_score(y_test_labels, predicted, average="macro"))
# print(recall_score(y_test_labels, predicted, average="macro"))

joblib.dump(classifier, 'topic_classifier_sklearn.pkl')




