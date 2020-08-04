#!/usr/bin/env bash

find /home/pi/airflow/logs/scheduler/* -type d -ctime +5 -exec rm -rf {} \;
find /home/pi/airflow/logs/news_grab/* -type d -ctime +5 -exec rm -rf {} \;
find /home/pi/airflow/logs/git_pull/* -type d -ctime +5 -exec rm -rf {} \;
find /home/pi/airflow/logs/update_servers/* -type d -ctime +5 -exec rm -rf {} \;