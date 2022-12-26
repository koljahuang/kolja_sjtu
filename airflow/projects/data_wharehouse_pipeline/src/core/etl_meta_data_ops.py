import logging
import re
import os,sys
import datetime
import subprocess
import traceback
from typing import *
from config import settings
from src.util.DtFormat import DtUtil
from src.util.deco import conn_cursor_wrapper, process_duration

LOG_NAME = settings.LOG_NAME
module_logger = logging.getLogger(f'{LOG_NAME}')


def query(etl_meta_db: str, sql: str) -> tuple or None:

    @conn_cursor_wrapper(etl_meta_db)
    def __query(conn, cursor, sql) -> tuple:
        module_logger.info(f"__query sql: {sql}")
        cursor.execute(sql)
        return cursor.fetchall()

    return __query(conn=None, cursor=None, sql=sql)


def gen_flow_files(etl_meta_db: str, binlog_topic: str,
                   binlog_path: str, max_new_files: str, max_hours: str) -> list:
    '''
    @param `binlog_path`: `hdfs://sjtu-datalake/kafka/binlog/topics`
    '''
    sql = '''SELECT 
                day dt, hr, count(*)
            FROM etl_flow_file 
            GROUP BY day, hr
            ORDER BY day desc, hr desc
            LIMIT 2'''

    # tuple:(('2021-11-24', '00', 3), ('2021-11-23', '23', 180))
    module_logger.info('Checking existence of files in the nearest recorded hourly time period from now')
    latest_dt_hr_file_cnt = query(etl_meta_db, sql)
    if not latest_dt_hr_file_cnt:
        module_logger.info(f'Files not exist in the nearest recorded hourly time period from now')
        module_logger.info(f'Getting first 100 files from hdfs path of binlog files')
        return get_first_hdfs_binlog_files(binlog_path, binlog_topic)

    new_files = []
    latest_dt_of_etl_flow_file = ''
    latest_hr_of_etl_flow_file = ''

    # from new to old scan unprocessed binlog files.
    # cover last two hours casue late data(kafka to hdfs) may occur.
    module_logger.info(f'Files exist in the nearest recorded hourly time period from now')
    module_logger.info(f'Scaning unprocessed binlog files each time period from front to back')
    for (d, h, cn) in latest_dt_hr_file_cnt:
        if not latest_dt_of_etl_flow_file:
            latest_dt_of_etl_flow_file = d
            latest_hr_of_etl_flow_file = h
        new_files += get_specified_hr_unprocessed_flow_files(
            etl_meta_db, binlog_path, binlog_topic, d, h)

    # TODO loop next hour relative to latest hr in `etl_flow_file` to get files, until reach the boundary value of max_hours or max_new_files .
    target_dt = latest_dt_of_etl_flow_file
    target_hr = latest_hr_of_etl_flow_file
    for i in range(0, max_hours):
        if (len(new_files) > max_new_files):
            break
        target_dt, target_hr = hour_later(target_dt, target_hr)
        new_files.extend(get_specified_hr_hdfs_binlog_files(
            binlog_path, binlog_topic, target_dt, target_hr))
        module_logger.info(
            f'{target_dt} {target_hr} new file : {len(new_files)}')
    # current num of new_files may be larger than max_new_files
    # filter the max_new_files to be marked
    return new_files[:max_new_files]


def get_first_hdfs_binlog_files(binlog_path: str, binlog_topic: str) -> list:
    cmd = "hdfs dfs -ls -t -R %s/%s | awk '!/^d/' | head -n 101 | awk -F ' ' '{print $8}'"%(binlog_path, binlog_topic)
    module_logger.info(f'cmd: {cmd}')
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, universal_newlines=True)
    output = p.communicate()[0]
    hdfs_files = re.findall(r'hdfs:.*', output)
    module_logger.debug(f'Get hdfs binlog files: {hdfs_files}')
    return hdfs_files


def get_specified_hr_unprocessed_flow_files(etl_meta_db: str,
                                            binlog_path: str,
                                            binlog_topic: str,
                                            dt: str,
                                            hr: str) -> list:
    new_files = []
    binlog_files = get_specified_hr_hdfs_binlog_files(
        binlog_path, binlog_topic, dt, hr)
    if not len(binlog_files):
        return new_files

    flow_files = query(
        etl_meta_db, f"SELECT file from etl_flow_file where day='{dt}' and hr='{hr}'")
    flow_files = [tup[0] for tup in flow_files]
    module_logger.info(f'processed in flow files : {len(flow_files)}')

    # TODO compare hdfs files and flow files to list unprocess files
    for file in binlog_files:
        if file not in flow_files:
            new_files.append(file)
    module_logger.info(f'{dt} {hr} new file : {len(new_files)}')
    return new_files


@process_duration
def get_specified_hr_hdfs_binlog_files(binlog_path: str, binlog_topic: str, dt: str, hr: str):
    year, mth, day = dt.split('-')
    url = os.path.join(binlog_path, binlog_topic, year, mth, day, hr)
    p = subprocess.Popen(f'hdfs dfs -ls {url}', shell=True, stdout=subprocess.PIPE, universal_newlines=True)
    output = p.communicate()[0]
    hdfs_files = re.findall(r'hdfs:.*', output)
    return hdfs_files


def hour_later(dt: str, hr: str) -> Tuple[str, str]:
    dt_hr = datetime.datetime.strptime(dt + " " + hr, "%Y-%m-%d %H")
    hour_later = dt_hr + datetime.timedelta(hours=1)
    return (datetime.datetime.strftime(hour_later, "%Y-%m-%d"), datetime.datetime.strftime(hour_later, "%H"))


def load_flow_files(etl_meta_db: str, flow_name: str, flow_id: str, flow_files) -> None:

    def split_dt_hr(file):
        try:
            ss = file.split('/')
            dt = ss[-5] + "-" + ss[-4] + "-" + ss[-3]
            hr = ss[-2]
        except Exception as e:
            traceback.print_exc()
            sys.exit(1)
        else:
            return (dt, hr)

    @conn_cursor_wrapper(etl_meta_db)
    def __load_flow_files(conn, cursor, sql) -> None:
        module_logger.info(f'__load_flow_files: {sql}')
        cursor.executemany(sql, values)
        conn.commit()

    create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    values = []
    for file in flow_files:
        dt, hr = split_dt_hr(file)
        values.append((file, create_time, dt, hr, flow_name, flow_id))

    sql = '''insert into etl_flow_file(file, create_time, day, hr, flow_name, flow_id, status) values(%s, %s, %s, %s, %s, %s,"INSERT" )'''
    __load_flow_files(conn=None, cursor=None, sql=sql)


def processing_flow_files(etl_meta_db: str, flow_id: str) -> Tuple[list, list]:
    '''
    Because files had been produced and loaded before, there must be a file on this flow_id;
    I am marking PROCESSING regardless of the status of these very files...
    '''
    values = query(
        etl_meta_db, f'select id, file from etl_flow_file where flow_id = {flow_id}')
    ids = [value[0] for value in values]
    files = [value[1] for value in values]
    flowid_status_mapping = [('PROCESSING', value[0],) for value in values]
    module_logger.info(f'processing flow files: {len(files)} ')
    module_logger.info(f'processing flow files: {flowid_status_mapping} ')
    return files, ids


def update_flow_file_status(etl_meta_db: str, flowid_status_mapping: tuple) -> None:
    @conn_cursor_wrapper(etl_meta_db)
    def __update_flow_file_status(conn, cursor, sql) -> None:
        module_logger.info(f'__update_flow_file_status: {sql}')
        cursor.executemany(sql, flowid_status_mapping)
        conn.commit()

    sql = '''update etl_flow_file set status=%s where id=%s '''
    
    __update_flow_file_status(conn=None, cursor=None, sql=sql)


def update_task_status(etl_meta_db: str,
                       flow_name: str,
                       flow_id: str,
                       dt_hr_ts_list: List[Tuple[Tuple[str, str], int]],
                       g_table_result: list) -> None:

    @conn_cursor_wrapper(etl_meta_db)
    def __update_task_status(conn, cursor, sql):

        def __insert_etl_flow_data_date(conn, cursor, sql) -> None:
            '''
            etl flow execution log
            update_time in table etl_flow_data_date is useless
            '''
            module_logger.info(f'__update_etl_flow_data_date: {sql}')
            cursor.executemany(sql, values_etl_flow_data_date)
            conn.commit()

        def __insert_etl_flow_table_result(conn, cursor, sql) -> None:
            module_logger.info(f'__insert_etl_flow_table_result: {sql}')
            cursor.executemany(sql, values_etl_flow_table_result)
            conn.commit()

        sql = "insert into etl_flow_data_date(flow_name, flow_id, day,hr, log_time, create_time) values(%s,%s,%s,%s,%s,%s)"
        __insert_etl_flow_data_date(conn, cursor, sql)
        sql = "insert into etl_flow_table_result(flow_name, flow_id, table_name, status, create_time) values(%s,%s,%s,%s,%s)"
        __insert_etl_flow_table_result(conn, cursor, sql)

    create_time = datetime.datetime.now().strftime('%Y-%m-%d %X')
    values_etl_flow_data_date = []
    for (dt, hr), ts in dt_hr_ts_list:
        # max ts in batch of json files
        log_time = DtUtil.timestampTostr(ts, "%Y-%m-%d %X")
        # log_time（max ts） and create_time won't be too far apart
        values_etl_flow_data_date.append(
            (flow_name, flow_id, dt, hr, log_time, create_time))
    module_logger.info(f'update data date: {values_etl_flow_data_date}')

    values_etl_flow_table_result = [
        (flow_name, flow_id, table, status, create_time) for table, status in g_table_result.items()]

    __update_task_status(conn=None, cursor=None, sql=None)
