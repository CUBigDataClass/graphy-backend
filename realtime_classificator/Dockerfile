FROM frolvlad/alpine-python-machinelearning:latest
ADD requirements.txt /
RUN pip install -r requirements.txt
ADD topic_classifier_sklearn_updated.pkl /
ADD sentiment_classifier_sklearn.pkl /
ADD current_topic_categories.txt /
ADD current_sentiment_categories.txt /
ADD classifier.py /


ENTRYPOINT ["python", "./classifier.py"]