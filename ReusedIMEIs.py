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

TIME_ZONE = 'Asia/Riyadh'
DATABASE_URL = 'http://ksazain-hb1:8765'
OPERATOR = '420_4'

IMEI_SUSPECT_MIN = 3
HOURLY_SUSPECT_DAYS = 3
THREAD_COUNT = 48
RETRY_INTERVAL = 30

LOGGER = logging.getLogger('ReusedIMEIs')
LOGGER.setLevel(logging.DEBUG)
file_handler = TimedRotatingFileHandler('/opt/pythonScripts/reusedIMEIs/log/ReusedIMEIs.log', when='midnight', backupCount=7)
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

@retry(wait=wait_fixed(RETRY_INTERVAL), stop=stop_after_attempt(4), before_sleep=before_sleep_log(LOGGER, logging.INFO))
def get_reused_imei():
    conn = phoenixdb.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.itersize = 1000000
    date_hour_limit = get_past_local_time(HOURLY_SUSPECT_DAYS, '%Y%m%d%H')

    LOGGER.info(date_hour_limit)

    sql = """select distinct S_IMEI from FRAUD_{}.HOURLY_SUSPECT where TR_DT_HR>={} group by s_imei having count(s_msisdn)>=3""".format(OPERATOR, date_hour_limit)
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
        cur.execute("""UPSERT INTO fraud_{}.hotimei (insert_time, imei, user, description) values (TO_TIMESTAMP(?), ?, 'SYSTEM', 'Reused IMEI') """.format(OPERATOR), (insert_time,imei))
    conn.close()


if __name__ == '__main__':
    # First get the IMEI
    imei_suspects = set()
    imei_suspects = get_reused_imei().toList()
    LOGGER.info('Number of IMEI suspects from FMS Detection Details: {}'.format(len(imei_suspects)))

    # Add to HOT IMEI
    add_to_hot_imei(imei_suspects)