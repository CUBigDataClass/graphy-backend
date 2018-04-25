FROM python:2.7-alpine

ADD requirements.txt /
RUN pip install -r requirements.txt
ADD application.py /

CMD gunicorn -w 2 -b 0.0.0.0:5000 application:app