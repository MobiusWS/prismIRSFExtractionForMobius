#!/opt/rh/rh-python36/root/usr/bin/python3 -W ignore

import os
import traceback
import re
from datetime import datetime, timedelta
import pytz
import logging
from logging.handlers import TimedRotatingFileHandler
from hdfs import InsecureClient
import pandas as pd
from pandas import errors
import phoenixdb
from threading import Thread
from queue import Queue
from collections import deque
from tenacity import retry, wait_fixed, stop_after_attempt, before_sleep_log


TIME_ZONE = 'Africa/Algiers'

OPERATOR = '603_3'
BLACK_LIST_FILE = None
WHITE_LIST_FILE = '/user/hadoop/whitelist_603_3.txt'
WEBHDFS_URL = 'http://oored-hb1:50070'
DATABASE_URL = 'http://oored-hb3:8765'

IMEI_SUSPECT_MIN = 3
DETECTION_DETAILS_DAYS = 10
THREAD_COUNT = 48
RETRY_INTERVAL = 30

LOGGER = logging.getLogger('IMEIUsingFMS')
LOGGER.setLevel(logging.DEBUG)
file_handler = TimedRotatingFileHandler('/home/mwsadmin/log/IMEIUsingFMS.log', when='midnight', backupCount=7)
file_handler.setFormatter(logging.Formatter('%(asctime)s [%(threadName)s, %(levelname)s]: %(message)s'))
LOGGER.addHandler(file_handler)
LOGGER.propagate = False

def get_local_time(_format):
    local_time = datetime.now().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(TIME_ZONE))
    return local_time.strftime(_format)


def get_past_local_time(days_ago, _format):
    current_time = get_local_time(_format)
    past_time = datetime.strptime(current_time, _format) - timedelta(days=days_ago)
    return past_time.strftime(_format)

@retry(wait=wait_fixed(RETRY_INTERVAL), stop=stop_after_attempt(1), before_sleep=before_sleep_log(LOGGER, logging.INFO))
def list_hdfs_dir(_dir_path):
    return client_hdfs.list(_dir_path)

@retry(wait=wait_fixed(RETRY_INTERVAL), stop=stop_after_attempt(1), before_sleep=before_sleep_log(LOGGER, logging.INFO))
def read_hdfs_file(_file_path):
    _white_list = []
    with client_hdfs.read(_file_path, encoding='utf-8') as f:
        for line in f.readlines():
            _white_list.append(line.rstrip())

    return _white_list

@retry(wait=wait_fixed(RETRY_INTERVAL), stop=stop_after_attempt(1), before_sleep=before_sleep_log(LOGGER, logging.INFO))
def get_white_list():
    # Get white list from database
    sql = 'select msisdn from fraud_{}.white_list_info where TYPE != 1'.format(OPERATOR)
    conn = phoenixdb.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.itersize = 1000000
    cursor.execute(sql)
    result = cursor.fetchall()
    db_white_list = set([_row[0] for _row in result])
    LOGGER.info('Number of white list numbers from database: {}'.format(len(db_white_list)))

    # Get white list from file
    file_white_list = []
    if WHITE_LIST_FILE is not None:
        file_white_list = read_hdfs_file(WHITE_LIST_FILE)

    LOGGER.info('Number of white list numbers from file: {}'.format(len(file_white_list)))
    return db_white_list.union(set(file_white_list))


@retry(wait=wait_fixed(RETRY_INTERVAL), stop=stop_after_attempt(4), before_sleep=before_sleep_log(LOGGER, logging.INFO))
def get_white_list_regex():
    sql = 'select msisdn from fraud_{}.white_list_info where TYPE = 1'.format(OPERATOR)
    conn = phoenixdb.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.itersize = 1000000
    cursor.execute(sql)
    result = cursor.fetchall()
    regex = '|'.join([_row[0] for _row in result])
    LOGGER.info('White List RegEx: "{}"'.format(regex))
    return regex


@retry(wait=wait_fixed(RETRY_INTERVAL), stop=stop_after_attempt(4), before_sleep=before_sleep_log(LOGGER, logging.INFO))
def get_fms_imei():
    conn = phoenixdb.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.itersize = 1000000
    date_hour_limit = get_past_local_time(DETECTION_DETAILS_DAYS, '%Y%m%d%H')

    LOGGER.info(date_hour_limit)

    sql = """select distinct imei from fraud_{}.detection_details where first_flag=1 and imei != '0' and call_date_hour >= '{}' and  source = 'FMS' and source is not null and msisdn not in (select msisdn from fraud_{}.white_list_info where TYPE !=1.format(OPERATOR) """.format(OPERATOR, date_hour_limit)
    print(sql)
    LOGGER.info('sql is : '.sql)

    cursor.execute(sql)
    result = cursor.fetchall()
    return {_row[0] for _row in result}


@retry(wait=wait_fixed(RETRY_INTERVAL), stop=stop_after_attempt(4), before_sleep=before_sleep_log(LOGGER, logging.INFO))
def add_to_hot_imei(imei_list):
    insert_time = get_local_time('%Y-%m-%d %H:%M:%S.000')
    conn = phoenixdb.connect(DATABASE_URL, autocommit=True)
    cur = conn.cursor()

    for imei in imei_list:
        cur.execute("""UPSERT INTO fraud_{}.hotimei (insert_time, imei, user, description) values (TO_TIMESTAMP(?), ?, 'FMS', 'FMS IMEI') """.format(OPERATOR), (insert_time,imei))
    conn.close()



if __name__ == '__main__':

     client_hdfs = InsecureClient(WEBHDFS_URL)

#    detection_queue = Queue()
#    writer = DetectionWriter(detection_queue)
#    writer.start()

#    whiteListFile=os.listdir(WHITE_LIST_FILE)
#    if len(whiteListFile) >0:
#       white_list=get_white_list()
#        white_list_regex=get_white_list_regex()

     #First get the IMEI
     imei_suspects = set()
     imei_suspects=get_fms_imei().toList()
     LOGGER.info('Number of IMEI suspects from FMS Detection Details: {}'.format(len(imei_suspects)))

     #Add to HOT IMEI
     add_to_hot_imei(imei_suspects)
