#Deriving the latest base image
FROM python:3.9

COPY . /home

RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip3 install -r /home/requirements.txt

