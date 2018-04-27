FROM cogentlabs/spark-cassandra:1.6.2

ADD output_topics_into_cassandra.py /
ADD stopwords_twitter.txt /
RUN mkdir /NB_model_without_pipeline
ADD NB_model_without_pipeline/* /NB_model_without_pipeline/
ENV SPARK_MASTER_URL spark://spark-master:7077


CMD ${SPARK_HOME}/bin/spark-submit --packages datastax:spark-cassandra-connector:2.0.1-s_2.11 /output_topics_into_cassandra.py