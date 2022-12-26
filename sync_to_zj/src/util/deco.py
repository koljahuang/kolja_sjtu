import datetime as dt
import logging
import os
import MySQLdb
import jaydebeapi
import sshtunnel
from .log import log
from config import settings
from sshtunnel import SSHTunnelForwarder

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
            file_handler = logger_obj[2]

            run(logger)

            logger.removeHandler(console_handler)
            logger.removeHandler(file_handler)
        return do
    return log_inner


def conn_cursor_wrapper(principal: str, db: str) -> callable:
    '''
    principal: get host, user, pwd
    '''

    if principal.lower() == 'zjgyy': 
        host = settings.HOST_ZJGYY
        port=settings.PORT_ZJGYY
        user = settings.USER_ZJGYY
        pwd = settings.PWD_ZJGYY
    elif principal.lower() == 'zjls':
        host = settings.HOST_ZJLS
        port=settings.PORT_ZJLS
        user = settings.USER_ZJLS
        pwd = settings.PWD_ZJLS
    elif principal.lower() == 'sa':
        host = settings.HOST_SA
        port=settings.PORT_SA
        user = settings.USER_SA
        pwd = settings.PWD_SA

    def conn_cursor_inner(run: callable) -> callable: 
        def inner(*args, **kwargs):
            conn = kwargs['conn']
            cursor = kwargs['cursor']
            sql = kwargs['sql']
            if not conn:
                with MySQLdb.connect(host=host, port=port, user=user, passwd=pwd, db=db, connect_timeout=5, charset='utf8') as conn:
                        with conn.cursor() as cursor:
                            return run(conn, cursor, sql)
            else:
                module_logger.debug('conn & cursor could be reused...')
                return run(conn, cursor, sql)
        return inner
    return conn_cursor_inner



def ssh_conn_cursor_wrapper(principal: str, db: str) -> callable:
    '''
    principal: get host, user, pwd
    '''
    if principal.lower() == 'sa':
        host = settings.HOST_SA
        port = settings.PORT_SA
        user = settings.USER_SA
        pwd = settings.PWD_SA
        ssh_host = settings.SSH_HOST_SA
        ssh_pwd = settings.SSH_PWD_SA
        ssh_user = settings.SSH_USER_SA
    elif principal.lower() == 'zjgyy':
        host = settings.HOST_ZJGYY
        port = settings.PORT_ZJGYY
        user = settings.USER_ZJGYY
        pwd = settings.PWD_ZJGYY
        ssh_host = settings.SSH_HOST_ZJGYY
        ssh_pwd = settings.SSH_PWD_ZJGYY
        ssh_user = settings.SSH_USER_ZJGYY  
    elif principal.lower() == 'zjls':
        host = settings.HOST_ZJLS
        port = settings.PORT_ZJLS
        user = settings.USER_ZJLS
        pwd = settings.PWD_ZJLS
        ssh_host = settings.SSH_HOST_ZJLS
        ssh_pwd = settings.SSH_PWD_ZJLS
        ssh_user = settings.SSH_USER_ZJLS
    
    
    def conn_cursor_inner(run: callable) -> callable: 
        def inner(*args, **kwargs):
            conn = kwargs['conn']
            cursor = kwargs['cursor']
            sql = kwargs['sql']
            if not conn:
                with SSHTunnelForwarder(
                    (ssh_host, 22),
                    ssh_username=ssh_user,
                    ssh_password=ssh_pwd,
                    remote_bind_address=(host, port)
                ) as server:
                    module_logger.debug('ssh succ...')
                    with MySQLdb.connect(
                            host='127.0.0.1', 
                            port=server.local_bind_port,
                            user=user, 
                            passwd=pwd, 
                            db=db, 
                            connect_timeout=10
                    ) as conn:
                            with conn.cursor() as cursor:
                                return run(conn, cursor, sql)
            else:
                module_logger.debug('conn & cursor could be reused...')
                return run(conn, cursor, sql)
        return inner
    return conn_cursor_inner


def ssh_sqlserver_conn_cursor_wrapper(principal: str, db: str) -> callable:
    '''
    principal: get host, user, pwd
    '''
    if principal == 'rsc':
        host = settings.HOST_RSC
        por = settings.PORT_RSC
        user = settings.USER_RSC
        pwd = settings.PWD_RSC
        ssh_host = settings.SSH_HOST_RSC
        ssh_pwd = settings.SSH_PWD_RSC
        ssh_user = settings.SSH_USER_RSC

    def conn_cursor_inner(run: callable) -> callable: 
        def inner(*args, **kwargs):
            conn = kwargs['conn']
            cursor = kwargs['cursor']
            sql = kwargs['sql']
            if not conn:
                with sshtunnel.SSHTunnelForwarder(
                    (ssh_host, 22), 
                    ssh_username=ssh_user,
                    ssh_password=ssh_pwd,
                    remote_bind_address=(host, por)
                    ) as tunnel:
                    module_logger.debug('ssh Connected!!')
                    # Set the server and port to be the local SSH tunnel values
                    server = tunnel.local_bind_address[0]
                    port = tunnel.local_bind_address[1]

                    conn_string = f"""jdbc:sqlserver://{server}:{port};databaseName={db};queryTimeout=45"""

                    current_dir = os.path.dirname(os.path.abspath(__file__))
                    parent_dir = os.path.abspath(os.path.dirname(current_dir)+os.path.sep+".")

                    with jaydebeapi.connect(
                        "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                        conn_string,
                        [user, pwd],
                        os.path.join(parent_dir, 'lib', 'mssql-jdbc-7.2.2.jre8.jar')
                    ) as conn:
                        with conn.cursor() as cursor:
                            return run(conn, cursor, sql)
            else:
                module_logger.debug('sql server conn could be reused...')
                return run(conn, cursor, sql)
        return inner
    return conn_cursor_inner


def sqlserver_conn_cursor_wrapper(principal: str, db: str) -> callable:
    '''
    principal: get host, user, pwd
    '''
    if principal == 'rsc':
        host = settings.HOST_RSC
        port = settings.PORT_RSC
        user = settings.USER_RSC
        pwd = settings.PWD_RSC

    def conn_cursor_inner(run: callable) -> callable: 
        def inner(*args, **kwargs):
            conn = kwargs['conn']
            cursor = kwargs['cursor']
            sql = kwargs['sql']
            if not conn:
                conn_string = f"""jdbc:sqlserver://{host}:{port};databaseName={db};queryTimeout=45"""

                current_dir = os.path.dirname(os.path.abspath(__file__))
                parent_dir = os.path.abspath(os.path.dirname(current_dir)+os.path.sep+".")

                with jaydebeapi.connect(
                    "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    conn_string,
                    [user, pwd],
                    os.path.join(parent_dir, 'lib', 'mssql-jdbc-7.2.2.jre8.jar')
                ) as conn:
                    with conn.cursor() as cursor:
                        return run(conn, cursor, sql)
            else:
                module_logger.debug('sql server conn could be reused...')
                return run(conn, cursor, sql)
        return inner
    return conn_cursor_inner


def ssh_oracle_conn_cursor_wrapper(principal: str) -> callable:
    import oracledb

    if settings.current_env == 'DEV':
        oracledb.init_oracle_client('src/lib/instantclient_19_8')
    elif settings.current_env == 'PROD':
        oracledb.init_oracle_client()
        

    if principal == 'gxk':
        host = settings.HOST_GXK
        por = settings.PORT_GXK
        user = settings.USER_GXK
        pwd = settings.PWD_GXK
        ssh_host = settings.SSH_HOST_GXK
        ssh_pwd = settings.SSH_PWD_GXK
        ssh_user = settings.SSH_USER_GXK
        service_name = settings.SERVICE_NAME

    def conn_cursor_inner(run: callable) -> callable: 
        def inner(*args, **kwargs):
            conn = kwargs['conn']
            cursor = kwargs['cursor']
            sql = kwargs['sql']
            if not conn:
                with SSHTunnelForwarder(
                    (ssh_host, 22),
                    ssh_username=ssh_user,
                    ssh_password=ssh_pwd,
                    remote_bind_address=(host, por)
                ) as tunnel:
                    module_logger.debug('ssh succ...')
                    server = tunnel.local_bind_address[0]
                    port = tunnel.local_bind_address[1]
                    dsn = f'%s/{pwd}@{server}:{port}/{service_name}'%(user)
                    with oracledb.connect(dsn) as conn:
                            with conn.cursor() as cursor:
                                return run(conn, cursor, sql)
            else:
                module_logger.debug('orcle conn could be reused...')
                return run(conn, cursor, sql)
        return inner
    return conn_cursor_inner


def oracle_conn_cursor_wrapper(principal: str) -> callable:
    import oracledb

    if settings.current_env == 'DEV':
        oracledb.init_oracle_client('src/lib/instantclient_19_8')
    elif settings.current_env == 'PROD':
        oracledb.init_oracle_client()
        

    if principal == 'gxk':
        host = settings.HOST_GXK
        port = settings.PORT_GXK
        user = settings.USER_GXK
        pwd = settings.PWD_GXK
        service_name = settings.SERVICE_NAME

    def conn_cursor_inner(run: callable) -> callable: 
        def inner(*args, **kwargs):
            conn = kwargs['conn']
            cursor = kwargs['cursor']
            sql = kwargs['sql']
            if not conn:
                dsn = f'%s/{pwd}@{host}:{port}/{service_name}'%(user)
                with oracledb.connect(dsn) as conn:
                        with conn.cursor() as cursor:
                            return run(conn, cursor, sql)
            else:
                module_logger.debug('orcle conn could be reused...')
                return run(conn, cursor, sql)
        return inner
    return conn_cursor_inner