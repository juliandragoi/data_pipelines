#Deriving the latest base image
FROM python:3.9

COPY . /home

USER pi

RUN pip3 install -r /home/requirements.txt

