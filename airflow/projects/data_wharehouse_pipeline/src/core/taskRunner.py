import logging
import json
from config import settings
from pyspark.sql.functions import *

LOG_NAME = settings.LOG_NAME
module_logger = logging.getLogger(f'{LOG_NAME}')


class TaskRunner:

    def __init__(self) -> None:
        pass

    def start(self, configs) -> None:
        db_from_binlogs = configs.db_from_binlogs
        tbl_from_binlogs = configs.tbl_from_binlogs
        binlog_table = configs.db_from_binlogs + '.' + configs.tbl_from_binlogs
        hive_db = configs.hive_db
        hive_table = configs.hive_table
        partition_field = 'etl_time'
        spark = configs.spark
        source_rdd = configs.source_rdd
        flowid = configs.flow_id
        hive_table_cols = configs.hive_table_cols
        output_parall = configs.output_parall

        def __insert_into_hive_table(incr_df):
            nonlocal hive_table_cols
            module_logger.info(f'【{binlog_table}】hive_table_cols: {hive_table_cols}')
            binlog_table_cols = [f.name.lower() for f in incr_df.schema.fields]
            module_logger.info(f'【{binlog_table}】binlog_table_cols: {binlog_table_cols}')

            # check cols
            hive_table_cols_raw = hive_table_cols.copy()
            hive_table_cols.sort()
            binlog_table_cols.sort()
            if hive_table_cols != binlog_table_cols:
                err_col_hive = set(hive_table_cols) - set(binlog_table_cols)
                err_col_binlog = set(binlog_table_cols) - set(hive_table_cols)
                try:
                    err_col_binlog.remove('update_id')   # exclude cols in binlog file
                except:
                    pass
                module_logger.warning(f'【{binlog_table}】col in hive is {err_col_hive}, but in binlog is {err_col_binlog}')

                if err_col_binlog:
                    # TODO alert etl engineer
                    pass

                if err_col_hive:
                    for col in err_col_hive:
                        incr_df = incr_df.withColumn(col, lit(""))
            
            # control output nums
            partitions = output_parall
            if (partitions == -1):
                partitions = 3
            incr_df = incr_df.cache().select(hive_table_cols).coalesce(partitions)
            module_logger.info(f'【{binlog_table}】incr_df partitions: {incr_df.rdd.getNumPartitions()}')
            incr_df.createOrReplaceTempView(hive_table)

            # control partition column （hard coding）
            partition_cols = 'dt' + ',' + 'flowid'
            hive_table_cols = ['`%s`'%(f) for f in hive_table_cols]
            sql = f'''INSERT OVERWRITE TABLE {hive_db}.{hive_table} PARTITION ({partition_cols})
                select {','.join(hive_table_cols_raw)} from {hive_table}'''
            module_logger.info(f'【{binlog_table}】{sql}')
            spark.sql(sql)
            module_logger.info(f'【{binlog_table}】thread task finished...')

        module_logger.info(
            f'parsing data from binlog {binlog_table} to hive {hive_db + "." + hive_table}')

        target_rdd = source_rdd.filter(
            lambda x: x['database'] == db_from_binlogs and x['table'] == tbl_from_binlogs)

        # convert rdd to dataframe
        target_rdd = target_rdd.map(lambda x: json.dumps(x))
        incr_df = spark.read.json(target_rdd)
        incr_df = incr_df.selectExpr('data.*', 'type as etl_type', 'ts as etl_time', 'xid as etl_xid', 'etl_db', 'etl_table')\
            .withColumn('etl_time', from_unixtime(col('etl_time').cast('string')))\
            .withColumn('flowid', lit(flowid))\
            .withColumn('dt', to_date(col(partition_field)))
        '''
        data.* turns data like '{"database":"asset13","xid":6245790674,"data":{"update_id":20644225419,"db_create_time":"2021-10-09 04:00:00.020"}'
        to be df with schema [update_id, db_create_time]
        '''

        module_logger.info(f'【{binlog_table}】printSchema: ')
        incr_df.printSchema()
        if settings.current_env.upper() == 'DEV':
            incr_df.show()

        __insert_into_hive_table(incr_df)

    


