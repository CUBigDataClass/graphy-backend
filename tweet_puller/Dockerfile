FROM python:2.7-alpine

ADD requirements.txt /
RUN pip install -r requirements.txt
ADD main.py /

ENTRYPOINT ["python", "./main.py"]