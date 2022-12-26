import argparse
from operator import mod
import threading
import sys
import re
import json
import logging
import traceback
from typing import Tuple
from src.util.DtFormat import DtUtil
from src.util.deco import process_duration
from src.core.bdp_mysql_ops import is_first_flow_processed, lock_flow, set_flow_status, is_flow_locked, update_daily_flag
from src.core.etl_meta_data_ops import query, gen_flow_files, load_flow_files, processing_flow_files, update_task_status, update_flow_file_status
from src.core.io_ops import get_hive_tables, get_hive_tbl_cols, ExpandedProcessor
from src.common.connections.source.koljaSpark import SparkFactory
from src.core.taskRunner import TaskRunner
from config import settings

LOG_NAME = settings.LOG_NAME
module_logger = logging.getLogger(f'{LOG_NAME}')
ENABLE_DISTRIBUTED_LOCK = settings.ENABLE_DISTRIBUTED_LOCK

g_table_result = {}


@process_duration
def run_task(configs) -> None:
    try:
        configs.spark.sparkContext.setLogLevel(configs.log_level)
        TaskRunner().start(configs)
    except Exception as e:
        g_table_result[configs.hive_table] = "FAILED"
        module_logger.error(e)
        traceback.print_exc()
    else:
        g_table_result[configs.hive_table] = "SUCCESSED"


@process_duration
def end_task(configs) -> None:

    def __to_day_hour(line) -> Tuple[Tuple[str, str], int]:
        ts = line['ts']
        dt = DtUtil.timestampTostr(ts, '%Y-%m-%d')
        hr = DtUtil.timestampTostr(ts, '%H')
        # timeArray = time.localtime(ts)
        # dt = time.strftime("%Y-%m-%d", timeArray)
        # hr = time.strftime("%H", timeArray)
        return ((dt, hr), ts)

    def __max_dt_hr_ts(dt_hr_ts1, dt_hr_ts2) -> Tuple[Tuple[str, str], int]:
        '''
        key: get the max ts group by dt
        e.g.[(('2021-10-09', '12'), 1633752000), (('2021-10-09', '12'), 1633753000)] --> [(('2021-10-09', '12'), 1633753000)]
        '''
        if dt_hr_ts1 > dt_hr_ts2:
            return dt_hr_ts1
        else:
            return dt_hr_ts2

    dt_hr_ts_list = configs.source_rdd.map(__to_day_hour).reduceByKey(
        lambda x, y: __max_dt_hr_ts(x, y)).collect()
    module_logger.info(f'dt_hr_ts_list: {dt_hr_ts_list}')

    update_task_status(configs.database, configs.flow_name,
                       configs.flow_id, dt_hr_ts_list, g_table_result)
    
    # dag faield, triger airflow dag alert
    for k, v in g_table_result.items():
        if v != "SUCCESSED":
            module_logger.error(f"{k} has error.")
            sys.exit(1)
    
    update_daily_flag(configs.flow_name, configs.flow_id, dt_hr_ts_list)

    update_flow_file_status(configs.database, [("SUCCESSED",id) for id in configs.ids])



@process_duration
def run(cmd_args: dict) -> None:

    def __deser(data):
        try:
            py_obj = json.loads(data)
        except Exception as e:
            module_logger.error(f"error record: %s" % data)
            sys.exit(1)
        return py_obj

    def __add_etl_db_etl_tb(d: dict) -> dict:
        '''
        需要确认分库分表的连接符
        '''
        d["etl_db"] = d["database"]
        d["etl_table"] = d["table"]

        d["database"] = re.split("\d+$", d["database"])[0]
        d["table"] = re.split("_\d+$", d["table"])[0]
        return d

    def __dbs_tbs_limit(rdd, dbs: str, tbs: str) -> None:
        if dbs and tbs:
            module_logger.info("limited to databases " + dbs)
            module_logger.info("limited to tables " + tbs)
            rdd.filter(lambda x: x["database"] in dbs.split(',') and x["table"] in tbs.split(','))
        elif dbs:
            rdd.filter(lambda x: x["database"] in dbs.split(','))
        elif tbs:
            rdd.filter(lambda x: x["table"] in tbs.split(','))
        return rdd
    
    BDP_MYSQL_INSTANCE = settings.BDP_MYSQL_INSTANCE
    from airflow.hooks.base import BaseHook
    air_conn = BaseHook.get_connection(BDP_MYSQL_INSTANCE)
    HOST_BDP_MYSQL_INSTANCE = air_conn.host
    USER_BDP_MYSQL_INSTANCE = air_conn.login
    PWD_BDP_MYSQL_INSTANCE = air_conn.password
    settings.HOST_BDP_MYSQL_INSTANCE = HOST_BDP_MYSQL_INSTANCE
    settings.USER_BDP_MYSQL_INSTANCE = USER_BDP_MYSQL_INSTANCE
    settings.PWD_BDP_MYSQL_INSTANCE = PWD_BDP_MYSQL_INSTANCE

    BDP_HIVE_META_INSTANCE = settings.BDP_HIVE_META_INSTANCE
    air_conn = BaseHook.get_connection(BDP_HIVE_META_INSTANCE)
    HOST_BDP_HIVE_META_INSTANCE = air_conn.host
    USER_BDP_HIVE_META_INSTANCE = air_conn.login
    PWD_BDP_HIVE_META_INSTANCE = air_conn.password
    settings.HOST_BDP_HIVE_META_INSTANCE = HOST_BDP_HIVE_META_INSTANCE
    settings.USER_BDP_HIVE_META_INSTANCE = USER_BDP_HIVE_META_INSTANCE
    settings.PWD_BDP_HIVE_META_INSTANCE = PWD_BDP_HIVE_META_INSTANCE

    flow_name = cmd_args['flow_name']

    processed = is_first_flow_processed(flow_name)
    if processed:
        module_logger.info(f'{flow_name} is processed before!')
        if ENABLE_DISTRIBUTED_LOCK:
            module_logger.info(f'distributed lock is enabled')
            module_logger.info(f'【{flow_name}】is_flow_locked, checking...')
            locked = is_flow_locked(flow_name)
            if locked:
                module_logger.info(f'【{flow_name}】is locked')
                sys.exit('the flow is locked')
            elif not locked:
                module_logger.info(f'【{flow_name}】is not locked, locking...')
                lock_flow(flow_name)  
        else:
            module_logger.info(f'distributed lock is not enabled')

        module_logger.info(f'【{flow_name}】is setting flow status...')      
        set_flow_status(flow_name, 1)
    elif not processed:
        module_logger.info(f'{flow_name} is not processed before!')
        if ENABLE_DISTRIBUTED_LOCK:
            module_logger.info(f'distributed lock is enabled, locking')
            lock_flow(flow_name)
        else:
            module_logger.info(f'distributed lock is not enabled')

        module_logger.info(f'【{flow_name}】is setting flow status...')   
        set_flow_status(flow_name, 1)

    # TODO new files operation
    cmd_args['flow_id'] = ''.join(
        c for c in cmd_args['flow_id'] if c.isdigit())[0:14]

    # refill pack: load_flow_files
    flow_files = query(cmd_args['database'],
                       sql=f"select id, file from etl_flow_file where flow_id = '{cmd_args['flow_id']}'")
    if not flow_files:
        module_logger.info(f"There is no file in table of etl_flow_file at specified flow_id of {cmd_args['flow_id']} (time point)")
        module_logger.info(f"Generating flow file...")
        flow_files = gen_flow_files(cmd_args['database'],
                                    cmd_args['category'],
                                    cmd_args['binlog_path'],
                                    cmd_args['max_new_files'],
                                    cmd_args['max_hours'])
        if not flow_files:
            # avoid spark report error to script, we need init spark.
            with SparkFactory() as spark:
                pass
            sys.exit(0)
        module_logger.info(f'Loading {len(flow_files)} flow files')
        load_flow_files(cmd_args['database'],
                        cmd_args['flow_name'],
                        cmd_args['flow_id'],
                        flow_files)
    else:
        module_logger.info(f"There are {len(flow_files)} files in table of etl_flow_file at specified flow_id of {cmd_args['flow_id']} (time point)")
        module_logger.info(f'Generating and loading flow file is not necessary')

    with SparkFactory() as spark:
        spark.sparkContext.setLogLevel(cmd_args['log_level'])
        module_logger.info('Starting process flow files')
        files, ids = processing_flow_files(cmd_args['database'],
                                           cmd_args['flow_id'])

        # data clean
        module_logger.info('Data cleaning')
        source_rdd = spark.sparkContext.textFile(','.join(files)).repartition(cmd_args['input_parall'])
        source_rdd = source_rdd.map(__deser)
        source_rdd = source_rdd.filter(lambda x: x != None)
        source_rdd = source_rdd.map(lambda x: __add_etl_db_etl_tb(x)) # 同时处理了分库分表的名称问题
        source_rdd = __dbs_tbs_limit(
            source_rdd, cmd_args['filter_dbs'], cmd_args['filter_tbs'])
        source_rdd.cache()

        # expanded data preprocessing
        if cmd_args['process_fuction']:
            module_logger.info(
                "expanded data preprocessing functions: " + cmd_args['process_fuction'])
            process_fuction_list = cmd_args['process_fuction'].split(',')
            p = ExpandedProcessor()
            for fun in process_fuction_list:
                source_rdd = eval('p.' + fun)(source_rdd)

        tables_from_binlogs = source_rdd.map(lambda x: (
            x['database'], x['table'])).distinct().collect()  # [('db1', 'tb1'), ('db2', 'tb2')]
        tables_from_hive = get_hive_tables(cmd_args['hive_db'])

        module_logger.info(f'tables_from_binlogs: {tables_from_binlogs}')
        module_logger.info(f'tables_from_hive: {tables_from_hive}')

        thread_tasks = []
        for (db, table) in tables_from_binlogs:

            hive_table = f'{db}_{table}'

            if hive_table not in list(map(lambda x: x[1], tables_from_hive)):
                module_logger.warning(
                    'hive_table in ods layer has not been created!')
                # TODO alert etl engineer
                continue

            module_logger.info(
                f'map table from binlogs "{table}" to hive name "{hive_table}"')

            hive_table_cols = get_hive_tbl_cols(
                cmd_args['hive_db'], hive_table)

            cmd_args['db_from_binlogs'] = db
            cmd_args['tbl_from_binlogs'] = table
            cmd_args['hive_table'] = hive_table
            cmd_args['hive_table_cols'] = hive_table_cols
            cmd_args['ids'] = ids
            cmd_args['spark'] = spark
            cmd_args['source_rdd'] = source_rdd

            t = threading.Thread(target=run_task, args=(
                argparse.Namespace(**cmd_args), ))
            t.start()
            thread_tasks.append(t)

        module_logger.info("waiting thread tasks running...")
        for t in thread_tasks:
            t.join()

        end_task(argparse.Namespace(**cmd_args))

        module_logger.info(
            f"done tables {len(g_table_result)}: {g_table_result}")

        set_flow_status(flow_name, 0)
        module_logger.info(
            f"flow finished")
