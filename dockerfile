#Deriving the latest base image
FROM python:3.9

COPY . /home

RUN pip3 install --upgrade pip3
RUN pip3 install -r /home/requirements.txt

