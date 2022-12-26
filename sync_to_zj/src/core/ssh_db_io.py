import logging
import MySQLdb
from typing import Tuple
from config import settings
from src.util.deco import *


LOG_NAME = settings.LOG_NAME
module_logger = logging.getLogger(f'{LOG_NAME}')


def mysqlQuery(principal: str, db: str, sql: str) -> tuple or None:

    @ssh_conn_cursor_wrapper(principal, db)
    def __mysqlQuery(conn, cursor, sql) -> tuple:
        module_logger.info(f"__mysqlQuery sql: {sql}")
        cursor.execute(sql)
        return cursor.fetchall()

    return __mysqlQuery(conn=None, cursor=None, sql=sql)


def mysqlTwoQuery(principal: str, db: str, sql1: str, sql2: str) -> tuple or None:

    @ssh_conn_cursor_wrapper(principal, db)
    def __mysqlQuery(conn, cursor, sql) -> tuple:
        module_logger.info(f"__mysqlQuery sql: {sql}")
        cursor.execute(sql)
        return cursor.fetchall()

    @ssh_conn_cursor_wrapper(principal, db)
    def __mysqlQuery2(conn, cursor, sql) -> Tuple[Tuple, Tuple]:
        res1 = __mysqlQuery(conn=conn, cursor=cursor, sql=sql1)
        res2 = __mysqlQuery(conn=conn, cursor=cursor, sql=sql2)
        return res1, res2

    return __mysqlQuery2(conn=None, cursor=None, sql=None)


def sqlServerQuery(principal: str, db: str, sql: str):
    
    @ssh_sqlserver_conn_cursor_wrapper(principal, db)
    def __sqlServerQuery(conn, cursor, sql) -> tuple:
        module_logger.info(f"__sqlServerQuery sql: {sql}")
        cursor.execute(sql)
        return cursor.fetchall()

    return __sqlServerQuery(conn=None, cursor=None, sql=sql)


def oraclQuery(principal: str, sql: str) -> list:

    @ssh_oracle_conn_cursor_wrapper(principal)
    def __oraclQuery(conn, cursor, sql) -> tuple:
        module_logger.info(f"__oraclQuery sql: {sql}")
        cursor.execute(sql)
        return cursor.fetchall()

    return __oraclQuery(conn=None, cursor=None, sql=sql)


def oraclQueryWithDDL(principal: str, preDDL: str, sql: str) -> list:

    @ssh_oracle_conn_cursor_wrapper(principal)
    def __oracleQuery(conn, cursor, sql) -> tuple:
        module_logger.info(f"__oracleQuery preDDL: {preDDL}")
        module_logger.info(f"__oracleQuery sql: {sql}")
        cursor.execute(preDDL)
        cursor.execute(sql)
        return cursor.fetchall()

    return __oracleQuery(conn=None, cursor=None, sql=sql)























# CREATE TABLE `JG_JBXX_CAMPUS_TEST` (
#   `userid` varchar(12) CHARACTER SET utf8 NOT NULL COMMENT '??',
#   `name` varchar(50) CHARACTER SET utf8 DEFAULT NULL COMMENT '??',
#   `gender` varchar(10) CHARACTER SET utf8 DEFAULT NULL COMMENT '??',
#   `birthdate` varchar(20) CHARACTER SET utf8 DEFAULT NULL COMMENT '????',
#   `idcardcode` varchar(50) CHARACTER SET utf8 DEFAULT NULL COMMENT '???',
#   `nation` varchar(50) CHARACTER SET utf8 DEFAULT NULL COMMENT '??',
#   `nationcode` varchar(10) CHARACTER SET utf8 DEFAULT NULL COMMENT '????',
#   `partisan_assemble` varchar(128) CHARACTER SET utf8 DEFAULT NULL COMMENT '????',
#   `phone` varchar(50) CHARACTER SET utf8 DEFAULT NULL COMMENT '???',
#   `jaccount` varchar(100) CHARACTER SET utf8 DEFAULT NULL COMMENT 'jaccount',
#   `xlmc` varchar(50) CHARACTER SET utf8 DEFAULT NULL COMMENT '????',
#   `xwmc` varchar(50) CHARACTER SET utf8 DEFAULT NULL COMMENT '????',
#   `graduate_school` varchar(128) CHARACTER SET utf8 DEFAULT NULL COMMENT '???????',
#   `graduate_school_academic` varchar(128) CHARACTER SET utf8 DEFAULT NULL COMMENT '???????',
#   `facultycode` varchar(10) CHARACTER SET utf8 DEFAULT NULL COMMENT '??????',
#   `faculty` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '??????',
#   `superiorfacultycode` varchar(10) CHARACTER SET utf8 DEFAULT NULL COMMENT '??????',
#   `superiorfaculty` varchar(50) CHARACTER SET utf8 DEFAULT NULL COMMENT '??????',
#   `jzgmblc` char(50) CHARACTER SET utf8 DEFAULT NULL COMMENT '?????',
#   `gwmc` char(50) CHARACTER SET utf8 DEFAULT NULL COMMENT '??',
#   `bzmc` char(50) CHARACTER SET utf8 DEFAULT NULL COMMENT '????',
#   `print_mc` varchar(30) CHARACTER SET utf8 DEFAULT NULL COMMENT '??????',
#   `ryztm` varchar(3) CHARACTER SET utf8 DEFAULT NULL COMMENT '????',
#   `FactInTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '??????',
#   PRIMARY KEY (`userid`)
# ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
