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
INPUT_PATH = '/data/GK3/feeds/603_3/csvfiles_copy/stage_c/'
OUTPUT_PATH = '/data/GK3/feeds/603_3/csvfiles_copy/stage_c'
PROCESS_GK = True
PROCESS_NON_GK = True
SHUTDOWN_FILE_GK = '/user/hadoop/report_603_3/SbGSM_{}_MobiusNRT_GKPMLI14.txt'
SHUTDOWN_FILE_NON_GK = '/user/hadoop/report_603_3/SbGSM_{}_MobiusNRT_GKPMLI141.txt'
WEBHDFS_URL = 'http://oored-hb2:50070'
DATABASE_URL = 'http://oored-hb3:8765'
OPERATOR = '603_3'
BLACK_LIST_FILE = None
WHITE_LIST_FILE = '/user/hadoop/whitelist_603_3.txt'
IMEI_SUSPECT_MIN = 3
DETECTION_DETAILS_DAYS = 10
THREAD_COUNT = 48
RETRY_INTERVAL = 30

LOGGER = logging.getLogger('IdenticalMSISDNs')
LOGGER.setLevel(logging.DEBUG)
file_handler = TimedRotatingFileHandler('/home/mwsadmin/log/IdenticalMSISDNs.log', when='midnight', backupCount=7)
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
def add_to_detection_details(_row, _criteria, _shutdown_file):
    call_date_hour = get_local_time('%Y%m%d%H')
    insert_time = get_local_time('%Y-%m-%d %H:%M:%S.000')
    call_time = datetime.strptime(_row['call_time'].strip(), '%Y%m%d%H%M%S').replace(tzinfo=pytz.utc)\
        .astimezone(pytz.timezone(TIME_ZONE)).strftime('%Y-%m-%d %H:%M:%S.000')
    conn = phoenixdb.connect(DATABASE_URL, autocommit=True)
    cur = conn.cursor()

    cur.execute("""UPSERT INTO fraud_{}.detection_details (call_date_hour, msisdn, insert_time, call_time,
                   source, criteria, shutdown_file, imsi, imei, cell_id, lac) VALUES (?, ?, TO_TIMESTAMP(?),
                   TO_TIMESTAMP(?), 'GKP', ?, ?, ?, ?, ?, ?)""".format(OPERATOR), (
        call_date_hour,
        _row['served_msisdn'],
        insert_time,
        call_time,
        _criteria,
        _shutdown_file,
        _row['served_imsi'],
        _row['served_imei'],
        _row['s_ci'],
        _row['s_lac']))

    conn.close()


@retry(wait=wait_fixed(RETRY_INTERVAL), stop=stop_after_attempt(4), before_sleep=before_sleep_log(LOGGER, logging.INFO))
def add_to_shutdown_report(_row, _criteria, _shutdown_file):
    insert_time = get_local_time('%Y-%m-%d %H:%M:%S.000')
    traffic_hour = datetime.strptime(_row['call_time'].strip(), '%Y%m%d%H%M%S').replace(tzinfo=pytz.utc) \
        .astimezone(pytz.timezone(TIME_ZONE)).strftime('%Y%m%d%H')
    conn = phoenixdb.connect(DATABASE_URL, autocommit=True)
    cur = conn.cursor()

    cur.execute("""UPSERT INTO fraud_{}.shutdown_report (id, traffic_hour, msisdn, criteria, report_name, insert_time)
                   VALUES (NEXT VALUE FOR fraud_{}.seq_shutdown_report, ?, ?, ?, ?, TO_TIMESTAMP(?))""".format(
                OPERATOR, OPERATOR),
                (traffic_hour,
                 _row['served_msisdn'],
                 _criteria,
                 _shutdown_file,
                 insert_time))

    conn.close()


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


@retry(wait=wait_fixed(RETRY_INTERVAL), stop=stop_after_attempt(3), before_sleep=before_sleep_log(LOGGER, logging.INFO))
def add_to_hdfs_file(_file_path, data):
    if client_hdfs.status(_file_path, strict=False) is None:
        with client_hdfs.write(_file_path, encoding='utf-8') as writer:
            writer.write(data)
    else:
        with client_hdfs.write(_file_path, encoding='utf-8', append=True) as writer:
            writer.write(data)


def get_moc_calls(_file_path):
    cols = ['call_type', 's_ci', 's_lac', 'served_msisdn', 'other_msisdn', 'served_imsi', 'served_imei', 'call_time']
    calls = pd.read_csv(_file_path, dtype=str, usecols=cols)

    if not calls.empty:
        calls = calls.fillna('0')

    return calls[calls['call_type'] == '1']

def get_callForward_moc_calls(_file_path):
    cols = ['call_type', 's_ci', 's_lac', 'served_msisdn', 'other_msisdn', 'served_imsi', 'served_imei', 'call_time']
    calls = pd.read_csv(_file_path, dtype=str, usecols=cols)

    if not calls.empty:
        calls = calls.fillna('0')

    return calls[calls['call_type'] == '14']

@retry(wait=wait_fixed(RETRY_INTERVAL), stop=stop_after_attempt(4), before_sleep=before_sleep_log(LOGGER, logging.INFO))
def add_to_invalid_imei(_row, _type):
    conn = phoenixdb.connect(DATABASE_URL, autocommit=True)
    cur = conn.cursor()

    cur.execute("""UPSERT INTO fraud_{}.invalid_imei (imei, a_msisdn, b_msisdn, type, insert_time, call_time, cell_id, lac, imsi)
                   VALUES (?, ?, ?, ?, now(), ?, ?, ?, ?)""".format(OPERATOR), (
        _row['served_imei'],
        _row['served_msisdn'],
        _row['other_msisdn'],
        _type,
        _row['call_time'],
        _row['s_ci'],
        _row['s_lac'],
        _row['served_imsi']))

    conn.close()


@retry(wait=wait_fixed(RETRY_INTERVAL), stop=stop_after_attempt(4), before_sleep=before_sleep_log(LOGGER, logging.INFO))
def get_numbers_with_invalid_imei():
    sql = 'select distinct a_msisdn, call_time, cell_id, lac, imsi, imei from fraud_{}.invalid_imei'.format(OPERATOR)
    conn = phoenixdb.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.itersize = 1000000
    cursor.execute(sql)
    _df = pd.DataFrame(cursor.fetchall())
    if not _df.empty:
        _df.set_index(0, inplace=True, drop=False)
        _df.columns = ['served_msisdn', 'call_time', 's_ci', 's_lac', 'served_imsi', 'served_imei']
    return _df


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
def get_imei_suspects_gk():
    conn = phoenixdb.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.itersize = 1000000
    date_hour_limit = get_past_local_time(DETECTION_DETAILS_DAYS, '%Y%m%d%H')

    sql = """select imei from fraud_{}.detection_details where first_flag=1 and imei != '0' and call_date_hour >= '{}' and source like 'GK%'
             group by imei having count(imei) >= {}""".format(
        OPERATOR, date_hour_limit, IMEI_SUSPECT_MIN)

    cursor.execute(sql)
    result = cursor.fetchall()
    return {_row[0] for _row in result}


@retry(wait=wait_fixed(RETRY_INTERVAL), stop=stop_after_attempt(4), before_sleep=before_sleep_log(LOGGER, logging.INFO))
def get_imei_suspects_non_gk():
    conn = phoenixdb.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.itersize = 1000000
    date_hour_limit = get_past_local_time(DETECTION_DETAILS_DAYS, '%Y%m%d%H')

    sql = """select imei from fraud_{}.detection_details where first_flag=1 and imei != '0' and call_date_hour >= '{}' and
             source not like 'GK%' and source is not null group by imei having count(imei) >= 1""".format(
        OPERATOR, date_hour_limit)

    cursor.execute(sql)
    result = cursor.fetchall()
    return {_row[0] for _row in result}


class DetectionWorker(Thread):

    def __init__(self, threadID, queue, file_list):
        Thread.__init__(self)
        self.threadID = threadID
        self.name = 'Detector-{}'.format(threadID)
        self.queue = queue
        self.file_list = file_list

    #def __init__(self, threadID, queue, file_list, _imei_suspects_gk, _imei_suspects_non_gk):
        #Thread.__init__(self)
        #self.threadID = threadID
        #self.name = 'Detector-{}'.format(threadID)
        #self.queue = queue
        #self.file_list = file_list
        #self.imei_suspects_gk = _imei_suspects_gk
        #self.imei_suspects_non_gk = _imei_suspects_non_gk




    def run(self):
        for file_name in self.file_list:

            LOGGER.info('Processing file: {}'.format(file_name))
            file_path = os.path.join(INPUT_PATH, file_name)

            try:
                #moc_calls = get_moc_calls(file_path)
                cfw_moc_calls = get_callForward_moc_calls
                LOGGER.info('Shape of CFW MOC calls: {}'.format(cfw_moc_calls.shape))

                # Find calls with invalid imei and store in database
                invalid_cfw_calls = cfw_moc_calls[(cfw_moc_calls['served_msidn'] == cfw_moc_calls['other_msisdn'])]

                if not invalid_cfw_calls.empty:
                    LOGGER.info('Number of calls with invalid cfw: {}'.format(invalid_cfw_calls.shape[0]))
                    LOGGER.debug(invalid_cfw_calls)
                    for index, row in invalid_cfw_calls.iterrows():
                        LOGGER.info(row, 1)

                #if len(self.imei_suspects_gk) != 0:
                    #caller_suspects = get_numbers_with_invalid_imei()
                    #callees = moc_calls[['other_msisdn']].drop_duplicates()
                    #callees.set_index('other_msisdn', inplace=True)
                    #suspects = callees.join(caller_suspects, how='inner')
                    #if not suspects.empty:
                        #LOGGER.debug('GK Suspects: \n{}'.format(suspects))
                        #self.queue.put((suspects, True))

                #if len(self.imei_suspects_non_gk) != 0:
                    #non_gk_suspects_calls = moc_calls[moc_calls['served_imei'].isin(self.imei_suspects_non_gk)]
                    #if not non_gk_suspects_calls.empty:
                        #LOGGER.debug('NON-GK Suspects: \n{}'.format(non_gk_suspects_calls))
                        #self.queue.put((non_gk_suspects_calls, False))

            except errors.EmptyDataError:
                LOGGER.warning('Empty file: {}, skipping...'.format(file_path))

            if OUTPUT_PATH is None:
                os.remove(file_path)
            else:
                os.rename(file_path, os.path.join(OUTPUT_PATH, file_name))


if __name__ == '__main__':

    client_hdfs = InsecureClient(WEBHDFS_URL)

    #detection_queue = Queue()
    #writer = DetectionWriter(detection_queue)
    #writer.start()

    inputFiles = os.listdir(INPUT_PATH)
    if len(inputFiles) > 0:
       #white_list = get_white_list()
        #white_list_regex = get_white_list_regex()
        #if len(white_list_regex) == 0:
            #white_list_regex = 'NONE'

        #imei_suspects = set()
        #if PROCESS_GK:
            #imei_suspects = get_imei_suspects_gk()
            #LOGGER.info('Number of IMEI suspects from GK Detection Details: {}'.format(len(imei_suspects)))

        #non_gk_imei_supspects = set()
        #if PROCESS_NON_GK:
            #non_gk_imei_supspects = get_imei_suspects_non_gk()
            #LOGGER.info('Number of IMEI suspects from NON-GK Detection Details: {}'.format(len(non_gk_imei_supspects)))
       if __name__ == '__main__':

           client_hdfs = InsecureClient(WEBHDFS_URL)

           detection_queue = Queue()
           # writer = DetectionWriter(detection_queue)
           # writer.start()

           inputFiles = os.listdir(INPUT_PATH)
           if len(inputFiles) > 0:
               # white_list = get_white_list()
               # white_list_regex = get_white_list_regex()
               # if len(white_list_regex) == 0:
               # white_list_regex = 'NONE'

               # imei_suspects = set()
               # if PROCESS_GK:
               # imei_suspects = get_imei_suspects_gk()
               # LOGGER.info('Number of IMEI suspects from GK Detection Details: {}'.format(len(imei_suspects)))

               # non_gk_imei_supspects = set()
               # if PROCESS_NON_GK:
               # non_gk_imei_supspects = get_imei_suspects_non_gk()
               # LOGGER.info('Number of IMEI suspects from NON-GK Detection Details: {}'.format(len(non_gk_imei_supspects)))

               chop_count = len(inputFiles) // THREAD_COUNT
               file_count = chop_count * THREAD_COUNT
               if chop_count < 1:
                   chop_count = 1
                   file_count = len(inputFiles)

               LOGGER.info('Processing {} files.'.format(file_count))
               threads = []

               for i in range(THREAD_COUNT):
                   if not inputFiles:
                       break
                   detector = DetectionWorker(i, detection_queue, inputFiles[:chop_count])
                   detector.start()
                   threads.append(detector)
                   inputFiles = inputFiles[chop_count:]

               for t in threads:
                   t.join()

               LOGGER.info('Finished processing {} files.'.format(file_count))

           detection_queue.put((None, False))
           # writer.join()

