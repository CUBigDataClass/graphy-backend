FROM casassg/spark:2.2.1_v1

ADD output_topics_into_cassandra.py /
ADD stopwords_twitter.txt /
ADD ./NB_model_without_pipeline /NB_model_without_pipeline
ENV SPARK_MASTER_URL spark://spark-master:7077
CMD spark-submit --packages datastax:spark-cassandra-connector:2.0.1-s_2.11 /output_topics_into_cassandra.py