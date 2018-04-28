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
from sklearn.feature_extraction.text import TfidfTransformer

## Extract actual necessary words from the tweet (little pre-processing done here)
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
    # print(url_less_words)
    return url_less_words

## creating training data, using twitter data
def get_training_data():
    f= open('/Users/saumya/Desktop/Big_data_project/tweet_emotion_labelled_data.txt', 'r', encoding='utf-8')

    training_data = []

    for l in f.readlines():
        # print(l)
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
print(train_data[0])

df = pd.DataFrame(train_data)
df.columns = ['text_label','text_words']
X=df['text_words'].values
y=df['text_label'].values

## printing 4 sentiment classes  - 'anger','joy','neutral','sadness'
print(np.unique(y))
print(type(X))
print(X[:10])
print(y[:10])

## encoding text labels
le = preprocessing.LabelEncoder()
Y = le.fit_transform(y)
print(Y.shape)

## split X and y numpy arrays to train & test
# X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.2)
# print (X_train.shape, y_train.shape)
# print (X_test.shape, y_test.shape)


## stop words
f=open("stopwords_twitter.txt","r")
add_stopwords =[]
for l in f.readlines():
    add_stopwords.append(l.strip())
print(add_stopwords)

stopwords = text.ENGLISH_STOP_WORDS.union(add_stopwords)

##Creating pipeline with countvectorizer and classifier
classifier = Pipeline([
    ('vectorizer', CountVectorizer(ngram_range=(1,2),stop_words=stopwords,min_df=1,binary=True)),
    ('clf', MultinomialNB(alpha=1.0))])

classifier.fit(X, Y)
# predicted = classifier.predict(X_test)
# all_labels = le.inverse_transform(predicted)
#
# print(accuracy_score(y_test, predicted))
# print(precision_score(y_test, predicted, average="macro"))
# print(recall_score(y_test, predicted, average="macro"))

joblib.dump(classifier, 'sentiment_classifier_sklearn.pkl')




