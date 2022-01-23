#Deriving the latest base image
FROM python:3.9

COPY . /home

RUN pip3 install -r /home/requirements.txt

CMD [ "python3", "/home/news/get_news_rss.py"]
