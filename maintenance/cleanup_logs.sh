#!/usr/bin/env bash

sudo find /home/pi/airflow/logs/scheduler/* -mtime +7 -type d -exec rm -r {} \;
sudo find /home/pi/airflow/logs/get_news_rss/check_feed_status/* -mtime +7 -type d -exec rm -r {} \;
sudo find /home/pi/airflow/logs/get_news_rss/feeds_insert/* -mtime +7 -type d -exec rm -r {} \;
sudo find /home/pi/airflow/logs/get_news_rss/news_insert/* -mtime +7 -type d -exec rm -r {} \;
sudo find /home/pi/airflow/logs/get_news_rss/rrs_news/* -mtime +7 -type d -exec rm -r {} \;

