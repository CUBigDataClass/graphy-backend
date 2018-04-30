FROM frolvlad/alpine-python-machinelearning:latest

ADD requirements.txt /
RUN pip install -r requirements.txt
ADD trend_similarity.py /
ADD application.py /

CMD gunicorn -w 3 -b 0.0.0.0:5000 --timeout 180 application:app