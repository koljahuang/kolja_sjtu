import datetime as dt
import logging
import MySQLdb
from .log import log
from config import settings

LOG_NAME = settings.LOG_NAME
module_logger = logging.getLogger(f'{LOG_NAME}')


def process_duration(fn: callable) -> callable:
    def duration_express(*args, **kwargs):
        start_time = dt.datetime.now().timestamp()
        res = fn(*args, **kwargs)
        end_time = dt.datetime.now().timestamp()
        module_logger.debug('{} executing time: {} seconds'.format(
            fn.__name__, end_time - start_time))
        return res
    return duration_express


def log_wrapper(name: str) -> callable:
    def log_inner(run):
        def do():
            logger_obj = log(name)
            logger = logger_obj[0]
            console_handler = logger_obj[1]
            # file_handler = logger_obj[2]

            run(logger)

            logger.removeHandler(console_handler)
            # logger.removeHandler(file_handler)
        return do
    return log_inner


def conn_cursor_wrapper(db: str) -> callable:
    try:
        if db == 'metastore': # hive metastore in one mysql instance
            host = settings.HOST_BDP_HIVE_META_INSTANCE
            user = settings.USER_BDP_HIVE_META_INSTANCE
            pwd = settings.PWD_BDP_HIVE_META_INSTANCE
        else:
            host = settings.HOST_BDP_MYSQL_INSTANCE
            user = settings.USER_BDP_MYSQL_INSTANCE
            pwd = settings.PWD_BDP_MYSQL_INSTANCE
    except:
        pass
    def conn_cursor_inner(run: callable) -> callable: 
        def inner(*args, **kwargs):
            conn = kwargs['conn']
            cursor = kwargs['cursor']
            sql = kwargs['sql']
            if not conn:
                with MySQLdb.connect(host=host, user=user, passwd=pwd, db=db, connect_timeout=5) as conn:
                        with conn.cursor() as cursor:
                            return run(conn, cursor, sql)
            else:
                return run(conn, cursor, sql)
        return inner
    return conn_cursor_inner
