#!/usr/bin/env bash

find /home/pi/airflow/logs/scheduler/* -mtime +7 -type d -exec rmdir {} \;
