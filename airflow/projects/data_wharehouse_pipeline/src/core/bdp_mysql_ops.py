import logging
import datetime
from typing import *
from config import settings
from src.util.deco import conn_cursor_wrapper

LOG_NAME = settings.LOG_NAME
module_logger = logging.getLogger(f'{LOG_NAME}')

BDP_MYSQL_DB = settings.BDP_MYSQL_DB


@conn_cursor_wrapper(BDP_MYSQL_DB)
def get_flow_lock(conn, cursor, sql:str) -> 'None or tuple':
    '''
    locked=0 means this flow is not locked
    locked=1 means this flow is locked
    '''
    module_logger.info(sql)
    cursor.execute(sql)
    lock_status = cursor.fetchone()
    return lock_status


@conn_cursor_wrapper(BDP_MYSQL_DB)
def get_flow_status(conn, cursor, sql:str) -> 'None or tuple':
    '''
    status: 0->finish, 1->running, 2->suspend(deprecated)
    '''
    module_logger.info(sql)
    cursor.execute(sql)
    status = cursor.fetchone()[0]
    return status


def set_flow_status(flow_name: str, status: int) -> None:
    '''
    status: 0->finish, 1->running, 2->suspend(deprecated)
    The status of locked flow will be converted to suspend.
    '''
    @conn_cursor_wrapper(BDP_MYSQL_DB)
    def __set_flow_status(conn, cursor, sql:str) -> None:
        module_logger.info(f'__set_flow_status: {sql}')
        cursor.execute(sql)
        conn.commit()

    sql = f"insert {BDP_MYSQL_DB}.flow_status(flow_name, status) values('{flow_name}', {status})  ON DUPLICATE KEY UPDATE status = {status}"
    __set_flow_status(conn=None, cursor=None, sql=sql)


def is_first_flow_processed(flow_name: str) -> bool:
    '''
    status=None means this flow is first operated
    '''
    @conn_cursor_wrapper(BDP_MYSQL_DB)
    def __is_first_flow_processed(conn, cursor, sql) -> bool:
        flow_status = get_flow_status(
            conn=conn, cursor=cursor, sql=sql)
        # (0 finished, 1 running)
        if flow_status in (0,1):
            return True
        else: 
            return False

    sql = f"select status from {BDP_MYSQL_DB}.flow_status where flow_name='{flow_name}'"
    return __is_first_flow_processed(conn=None, cursor=None, sql=sql)


def lock_flow(flow_name: str) -> None:

    @conn_cursor_wrapper(BDP_MYSQL_DB)
    def __lock_flow(conn, cursor, sql) -> None:
        module_logger.info(sql)
        cursor.execute(sql)
        conn.commit()

    update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sql = f"insert into {BDP_MYSQL_DB}.flow_lock(flow_name, locked) values('{flow_name}', 1) ON DUPLICATE KEY UPDATE update_time ='{update_time}'"
    __lock_flow(conn=None, cursor=None, sql=sql)


def release_flow(flow_name: str) -> None:

    @conn_cursor_wrapper(BDP_MYSQL_DB)
    def __release_flow(conn, cursor, sql) -> None:
        module_logger.info(sql)
        cursor.execute(sql)
        conn.commit()

    update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sql = f"insert into {BDP_MYSQL_DB}.flow_lock(flow_name, locked) values('{flow_name}', 0) ON DUPLICATE KEY UPDATE update_time ='{update_time}'"
    __release_flow(conn=None, cursor=None, sql=sql)


def is_flow_locked(flow_name: str) -> bool:

    @conn_cursor_wrapper(BDP_MYSQL_DB)
    def __is_flow_locked(conn, cursor, sql) -> bool:
        lock_status = get_flow_lock(
            conn=conn, cursor=cursor, sql=sql)
        return lock_status[0] == 1

    sql = f"select locked from {BDP_MYSQL_DB}.flow_lock where flow_name='{flow_name}'"
    return __is_flow_locked(conn=None, cursor=None, sql=sql)


def update_daily_flag(flow_name: str, 
                      flow_id: str, 
                      dt_hr_ts_list: List[Tuple[Tuple[str, str], int]]
                    ) -> None:
    '''
    该函数的意义在于, airflow task完成后, 往flag表插入状态, dw层任务check完毕, 才可以进行.
    这里必须要考虑对于更新频率很高的mysql实例, 这样的更新很正常，如果有的实例很多天都不更新,
    就会拿不到ts的更新, 也就不会更新flag表, dw层dag就会阻塞

    所以dw层任务的check机制, 需要切换到:
    select
        sum(datediff(date(now()), dt)) v     -- v必须等于1
    from
        (
            select
                dt    
            from
                (
                    select 
                        distinct date(create_time) dt 
                    from mysql_instance_bnb_bdp.etl_flow_table_result 
                    where flow_name = '${flow_name}' 
                    and table_name = '${table_name}' 
                    and status = 'SUCCESSED' 
                )
            order by 
                dt
            limit 2
        )

    这里依然会往flag插入状态, 但可能拿不到状态，原因上面解释了
    '''
    @conn_cursor_wrapper(BDP_MYSQL_DB)
    def __update_daily_flag(conn, cursor, sql) -> None:
        module_logger.info(f'__update_daily_flag: {__update_daily_flag}')
        cursor.execute(sql)
        conn.commit()

    dt_list = [dt for ((dt, hr), ts) in dt_hr_ts_list]
    dt_list = sorted(dt_list, key=lambda x: x)
    latest_dt = dt_list[0]
    last_day = (datetime.datetime.strptime(latest_dt, '%Y-%m-%d') - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    sql = f"insert into flag(task_name, task_id, day) values('{flow_name}','{flow_id}','{last_day}') ON DUPLICATE KEY UPDATE update_time ='{update_time}'"
    __update_daily_flag(conn=None, cursor=None, sql=sql)