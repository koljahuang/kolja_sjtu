import logging
import MySQLdb
from typing import Tuple
from config import settings
from src.util.deco import *


LOG_NAME = settings.LOG_NAME
module_logger = logging.getLogger(f'{LOG_NAME}')


def mysqlQuery(principal: str, db: str, sql: str) -> tuple or None:

    @conn_cursor_wrapper(principal, db)
    def __mysqlQuery(conn, cursor, sql) -> tuple:
        module_logger.info(f"__mysqlQuery sql: {sql}")
        cursor.execute(sql)
        return cursor.fetchall()

    return __mysqlQuery(conn=None, cursor=None, sql=sql)


def mysqlTwoQuery(principal: str, db: str, sql1: str, sql2: str) -> tuple or None:

    @conn_cursor_wrapper(principal, db)
    def __mysqlQuery(conn, cursor, sql) -> tuple:
        module_logger.info(f"__mysqlQuery sql: {sql}")
        cursor.execute(sql)
        return cursor.fetchall()

    @conn_cursor_wrapper(principal, db)
    def __mysqlQuery2(conn, cursor, sql) -> Tuple[Tuple, Tuple]:
        res1 = __mysqlQuery(conn=conn, cursor=cursor, sql=sql1)
        res2 = __mysqlQuery(conn=conn, cursor=cursor, sql=sql2)
        return res1, res2

    return __mysqlQuery2(conn=None, cursor=None, sql=None)


def sqlServerQuery(principal: str, db: str, sql: str):
    
    @sqlserver_conn_cursor_wrapper(principal, db)
    def __sqlServerQuery(conn, cursor, sql) -> tuple:
        module_logger.info(f"__sqlServerQuery sql: {sql}")
        cursor.execute(sql)
        return cursor.fetchall()

    return __sqlServerQuery(conn=None, cursor=None, sql=sql)


def oraclQuery(principal: str, sql: str) -> list:

    @oracle_conn_cursor_wrapper(principal)
    def __oraclQuery(conn, cursor, sql) -> tuple:
        module_logger.info(f"__oraclQuery sql: {sql}")
        cursor.execute(sql)
        return cursor.fetchall()

    return __oraclQuery(conn=None, cursor=None, sql=sql)


def oraclQueryWithDDL(principal: str, preDDL: str, sql: str) -> list:

    @oracle_conn_cursor_wrapper(principal)
    def __oracleQuery(conn, cursor, sql) -> tuple:
        module_logger.info(f"__oracleQuery preDDL: {preDDL}")
        module_logger.info(f"__oracleQuery sql: {sql}")
        cursor.execute(preDDL)
        cursor.execute(sql)
        return cursor.fetchall()

    return __oracleQuery(conn=None, cursor=None, sql=sql)


def upsertDfTo_JG_JBXX_CAMPUS(principal: str, df, target_tbl: str) -> tuple or None:

    if settings.current_env == 'DEV':
        host = settings.HOST_LOCAL
        port=settings.PORT_LOCAL
        user = settings.USER_LOCAL
        pwd = settings.PWD_LOCAL
        db = 'test'
        module_logger.info(f'准备写入local test, {target_tbl}')
    elif settings.current_env == 'PROD':
        if principal.lower() == 'zjgyy': 
            host = settings.HOST_ZJGYY
            port = settings.PORT_ZJGYY
            user = settings.USER_ZJGYY
            pwd = settings.PWD_ZJGYY
            db = 'jsc'
        elif principal.lower() == 'zjls': 
            host = settings.HOST_ZJLS
            port = settings.PORT_ZJLS
            user = settings.USER_ZJLS
            pwd = settings.PWD_ZJLS
            db = 'jsc'
            module_logger.info(f'准备写入{principal}, {target_tbl}')
    
    with MySQLdb.connect(host=host, port=port, user=user, passwd=pwd, db=db, connect_timeout=10, charset='utf8') as conn:
        with conn.cursor() as cursor:
            for line in df.to_numpy().tolist():
                line = list(map(lambda x: '(NULL)' if x is None else x, line))
                sql = f"""
                insert into {target_tbl} (
                    userid,
                    name,                     
                    gender,                 
                    birthdate,                
                    idcardcode,               
                    nation,                   
                    nationcode,               
                    partisan_assemble,        
                    phone,                    
                    jaccount,                 
                    xlmc,                     
                    xwmc,                     
                    graduate_school,          
                    graduate_school_academic, 
                    facultycode,              
                    faculty,                  
                    superiorfacultycode,      
                    superiorfaculty,          
                    jzgmblc,                  
                    gwmc,                     
                    bzmc,                     
                    print_mc,                 
                    ryztm                 
                ) values (
                    "{line[0]}",
                    "{line[1]}",
                    "{line[2]}",
                    "{line[3]}",
                    "{line[4]}",
                    "{line[5]}",
                    "{line[6]}",
                    "{line[7]}",
                    "{line[8]}",
                    "{line[9]}",
                    "{line[10]}",
                    "{line[11]}",
                    "{line[12]}",
                    "{line[13]}",
                    "{line[14]}",
                    "{line[15]}",
                    "{line[16]}",
                    "{line[17]}",
                    "{line[18]}",
                    "{line[19]}",
                    "{line[20]}",
                    "{line[21]}",
                    "{line[22]}"
                ) ON DUPLICATE KEY UPDATE name = "{line[1]}",gender = "{line[2]}",birthdate = "{line[3]}",
                idcardcode = "{line[4]}",
                nation = "{line[5]}",
                nationcode = "{line[6]}",
                partisan_assemble = "{line[7]}",
                phone = "{line[8]}",
                jaccount = "{line[9]}",
                xlmc = "{line[10]}",
                xwmc = "{line[11]}",
                graduate_school = "{line[12]}",
                graduate_school_academic = "{line[13]}",
                facultycode = "{line[14]}",
                faculty = "{line[15]}",
                superiorfacultycode = "{line[16]}",
                superiorfaculty = "{line[17]}",
                jzgmblc = "{line[18]}",
                gwmc = "{line[19]}",
                bzmc = "{line[20]}",
                print_mc = "{line[21]}",
                ryztm = "{line[22]}"
                """
                cursor.execute(sql)
                conn.commit()


def upsertDfTo_XS_JBXX_CAMPUS(principal: str, df, target_tbl: str) -> tuple or None:

    if settings.current_env == 'DEV':
        host = settings.HOST_LOCAL
        port=settings.PORT_LOCAL
        user = settings.USER_LOCAL
        pwd = settings.PWD_LOCAL
        db = 'test'
        module_logger.info(f'准备写入local test, {target_tbl}')
    elif settings.current_env == 'PROD':
        if principal.lower() == 'zjgyy': 
            host = settings.HOST_ZJGYY
            port = settings.PORT_ZJGYY
            user = settings.USER_ZJGYY
            pwd = settings.PWD_ZJGYY
            db = 'jsc'
        elif principal.lower() == 'zjls': 
            host = settings.HOST_ZJLS
            port = settings.PORT_ZJLS
            user = settings.USER_ZJLS
            pwd = settings.PWD_ZJLS
            db = 'jsc'
            module_logger.info(f'准备写入{principal}, {target_tbl}')
    
    with MySQLdb.connect(host=host, port=port, user=user, passwd=pwd, db=db, connect_timeout=10, charset='utf8') as conn:
        with conn.cursor() as cursor:
            for line in df.to_numpy().tolist():
                line = list(map(lambda x: '(NULL)' if x is None else x, line))
                if line[45] != "(NULL)" and line[46] != "(NULL)":
                    sql = f"""
                    insert into {target_tbl} (
                        XH,
                        XM,
                        XMPY,
                        XBDM,
                        XBMC,
                        CSRQ,
                        MZDM,
                        MZMC,
                        ZZMMDM,
                        ZZMMMC,
                        JGDM,
                        JGMC,
                        HYZKMC,
                        GBDM,
                        GBMC,
                        SFZHM,
                        XLDM,
                        XLMC,
                        SYDQM,
                        SYDQMC,
                        JTDZ,
                        EMAIL,
                        SJ,
                        PYFSDM,
                        PYFSMC,
                        YXDM,
                        YXMC,
                        BH,
                        ZYDM,
                        ZYMC,
                        DSGH,
                        DSXM,
                        DSGH2,
                        DSXM2,
                        LXBJ,
                        LXBJMC,
                        XQDM,
                        SJLY,
                        XJZTMX,
                        XJZTMXMC,
                        XZ,
                        XXFSDM,
                        XXFSMC,
                        SFLXS,
                        YJBYSJ,
                        DBRQ,
                        BYSJ,
                        RXNJ,
                        BYXWDM,
                        BYXWMC,
                        SCBJ,
                        SJC               
                    ) values (
                        "{line[0]}",
                        "{line[1]}",
                        "{line[2]}",
                        "{line[3]}",
                        "{line[4]}",
                        "{line[5]}",
                        "{line[6]}",
                        "{line[7]}",
                        "{line[8]}",
                        "{line[9]}",
                        "{line[10]}",
                        "{line[11]}",
                        "{line[12]}",
                        "{line[13]}",
                        "{line[14]}",
                        "{line[15]}",
                        "{line[16]}",
                        "{line[17]}",
                        "{line[18]}",
                        "{line[19]}",
                        "{line[20]}",
                        "{line[21]}",
                        "{line[22]}",
                        "{line[23]}",
                        "{line[24]}",
                        "{line[25]}",
                        "{line[26]}",
                        "{line[27]}",
                        "{line[28]}",
                        "{line[29]}",
                        "{line[30]}",
                        "{line[31]}",
                        "{line[32]}",
                        "{line[33]}",
                        "{line[34]}",
                        "{line[35]}",
                        "{line[36]}",
                        "{line[37]}",
                        "{line[38]}",
                        "{line[39]}",
                        "{line[40]}",
                        "{line[41]}",
                        "{line[42]}",
                        "{line[43]}",
                        "{line[44]}",
                        "{line[45]}",
                        "{line[46]}",
                        "{line[47]}",
                        "{line[48]}",
                        "{line[49]}",
                        "{line[50]}",
                        "{line[51]}"
                    ) ON DUPLICATE KEY UPDATE XM = "{line[1]}",XMPY = "{line[2]}",XBDM = "{line[3]}",
                    XBMC = "{line[4]}",
                    CSRQ = "{line[5]}",
                    MZDM = "{line[6]}",
                    MZMC = "{line[7]}",
                    ZZMMDM = "{line[8]}",
                    ZZMMMC = "{line[9]}",
                    JGDM = "{line[10]}",
                    JGMC = "{line[11]}",
                    HYZKMC = "{line[12]}",
                    GBDM = "{line[13]}",
                    GBMC = "{line[14]}",
                    SFZHM = "{line[15]}",
                    XLDM = "{line[16]}",
                    XLMC = "{line[17]}",
                    SYDQM = "{line[18]}",
                    SYDQMC = "{line[19]}",
                    JTDZ = "{line[20]}",
                    EMAIL = "{line[21]}",
                    SJ = "{line[22]}",
                    PYFSDM = "{line[23]}",
                    PYFSMC = "{line[24]}",
                    YXDM = "{line[25]}",
                    YXMC = "{line[26]}",
                    BH = "{line[27]}",
                    ZYDM = "{line[28]}",
                    ZYMC = "{line[29]}",
                    DSGH = "{line[30]}",
                    DSXM = "{line[31]}",
                    DSGH2 = "{line[32]}",
                    DSXM2 = "{line[33]}",
                    LXBJ = "{line[34]}",
                    LXBJMC = "{line[35]}",
                    XQDM = "{line[36]}",
                    SJLY = "{line[37]}",
                    XJZTMX = "{line[38]}",
                    XJZTMXMC = "{line[39]}",
                    XZ = "{line[40]}",
                    XXFSDM = "{line[41]}",
                    XXFSMC = "{line[42]}",
                    SFLXS = "{line[43]}",
                    YJBYSJ = "{line[44]}",
                    DBRQ = "{line[45]}",
                    BYSJ = "{line[46]}",
                    RXNJ = "{line[47]}",
                    BYXWDM = "{line[48]}",
                    BYXWMC = "{line[49]}",
                    SCBJ = "{line[50]}",
                    SJC = "{line[51]}"
                    """
                elif line[45] == "(NULL)" and line[46] == "(NULL)":
                    sql = f"""
                    insert into {target_tbl} (
                        XH,
                        XM,
                        XMPY,
                        XBDM,
                        XBMC,
                        CSRQ,
                        MZDM,
                        MZMC,
                        ZZMMDM,
                        ZZMMMC,
                        JGDM,
                        JGMC,
                        HYZKMC,
                        GBDM,
                        GBMC,
                        SFZHM,
                        XLDM,
                        XLMC,
                        SYDQM,
                        SYDQMC,
                        JTDZ,
                        EMAIL,
                        SJ,
                        PYFSDM,
                        PYFSMC,
                        YXDM,
                        YXMC,
                        BH,
                        ZYDM,
                        ZYMC,
                        DSGH,
                        DSXM,
                        DSGH2,
                        DSXM2,
                        LXBJ,
                        LXBJMC,
                        XQDM,
                        SJLY,
                        XJZTMX,
                        XJZTMXMC,
                        XZ,
                        XXFSDM,
                        XXFSMC,
                        SFLXS,
                        YJBYSJ,
                        DBRQ,
                        BYSJ,
                        RXNJ,
                        BYXWDM,
                        BYXWMC,
                        SCBJ,
                        SJC               
                    ) values (
                        "{line[0]}",
                        "{line[1]}",
                        "{line[2]}",
                        "{line[3]}",
                        "{line[4]}",
                        "{line[5]}",
                        "{line[6]}",
                        "{line[7]}",
                        "{line[8]}",
                        "{line[9]}",
                        "{line[10]}",
                        "{line[11]}",
                        "{line[12]}",
                        "{line[13]}",
                        "{line[14]}",
                        "{line[15]}",
                        "{line[16]}",
                        "{line[17]}",
                        "{line[18]}",
                        "{line[19]}",
                        "{line[20]}",
                        "{line[21]}",
                        "{line[22]}",
                        "{line[23]}",
                        "{line[24]}",
                        "{line[25]}",
                        "{line[26]}",
                        "{line[27]}",
                        "{line[28]}",
                        "{line[29]}",
                        "{line[30]}",
                        "{line[31]}",
                        "{line[32]}",
                        "{line[33]}",
                        "{line[34]}",
                        "{line[35]}",
                        "{line[36]}",
                        "{line[37]}",
                        "{line[38]}",
                        "{line[39]}",
                        "{line[40]}",
                        "{line[41]}",
                        "{line[42]}",
                        "{line[43]}",
                        "{line[44]}",
                        null,
                        null,
                        "{line[47]}",
                        "{line[48]}",
                        "{line[49]}",
                        "{line[50]}",
                        "{line[51]}"
                    ) ON DUPLICATE KEY UPDATE XM = "{line[1]}",XMPY = "{line[2]}",XBDM = "{line[3]}",
                    XBMC = "{line[4]}",
                    CSRQ = "{line[5]}",
                    MZDM = "{line[6]}",
                    MZMC = "{line[7]}",
                    ZZMMDM = "{line[8]}",
                    ZZMMMC = "{line[9]}",
                    JGDM = "{line[10]}",
                    JGMC = "{line[11]}",
                    HYZKMC = "{line[12]}",
                    GBDM = "{line[13]}",
                    GBMC = "{line[14]}",
                    SFZHM = "{line[15]}",
                    XLDM = "{line[16]}",
                    XLMC = "{line[17]}",
                    SYDQM = "{line[18]}",
                    SYDQMC = "{line[19]}",
                    JTDZ = "{line[20]}",
                    EMAIL = "{line[21]}",
                    SJ = "{line[22]}",
                    PYFSDM = "{line[23]}",
                    PYFSMC = "{line[24]}",
                    YXDM = "{line[25]}",
                    YXMC = "{line[26]}",
                    BH = "{line[27]}",
                    ZYDM = "{line[28]}",
                    ZYMC = "{line[29]}",
                    DSGH = "{line[30]}",
                    DSXM = "{line[31]}",
                    DSGH2 = "{line[32]}",
                    DSXM2 = "{line[33]}",
                    LXBJ = "{line[34]}",
                    LXBJMC = "{line[35]}",
                    XQDM = "{line[36]}",
                    SJLY = "{line[37]}",
                    XJZTMX = "{line[38]}",
                    XJZTMXMC = "{line[39]}",
                    XZ = "{line[40]}",
                    XXFSDM = "{line[41]}",
                    XXFSMC = "{line[42]}",
                    SFLXS = "{line[43]}",
                    YJBYSJ = "{line[44]}",
                    DBRQ = null,
                    BYSJ = null,
                    RXNJ = "{line[47]}",
                    BYXWDM = "{line[48]}",
                    BYXWMC = "{line[49]}",
                    SCBJ = "{line[50]}",
                    SJC = "{line[51]}"
                    """
                elif line[45] == "(NULL)":
                    sql = f"""
                    insert into {target_tbl} (
                        XH,
                        XM,
                        XMPY,
                        XBDM,
                        XBMC,
                        CSRQ,
                        MZDM,
                        MZMC,
                        ZZMMDM,
                        ZZMMMC,
                        JGDM,
                        JGMC,
                        HYZKMC,
                        GBDM,
                        GBMC,
                        SFZHM,
                        XLDM,
                        XLMC,
                        SYDQM,
                        SYDQMC,
                        JTDZ,
                        EMAIL,
                        SJ,
                        PYFSDM,
                        PYFSMC,
                        YXDM,
                        YXMC,
                        BH,
                        ZYDM,
                        ZYMC,
                        DSGH,
                        DSXM,
                        DSGH2,
                        DSXM2,
                        LXBJ,
                        LXBJMC,
                        XQDM,
                        SJLY,
                        XJZTMX,
                        XJZTMXMC,
                        XZ,
                        XXFSDM,
                        XXFSMC,
                        SFLXS,
                        YJBYSJ,
                        DBRQ,
                        BYSJ,
                        RXNJ,
                        BYXWDM,
                        BYXWMC,
                        SCBJ,
                        SJC               
                    ) values (
                        "{line[0]}",
                        "{line[1]}",
                        "{line[2]}",
                        "{line[3]}",
                        "{line[4]}",
                        "{line[5]}",
                        "{line[6]}",
                        "{line[7]}",
                        "{line[8]}",
                        "{line[9]}",
                        "{line[10]}",
                        "{line[11]}",
                        "{line[12]}",
                        "{line[13]}",
                        "{line[14]}",
                        "{line[15]}",
                        "{line[16]}",
                        "{line[17]}",
                        "{line[18]}",
                        "{line[19]}",
                        "{line[20]}",
                        "{line[21]}",
                        "{line[22]}",
                        "{line[23]}",
                        "{line[24]}",
                        "{line[25]}",
                        "{line[26]}",
                        "{line[27]}",
                        "{line[28]}",
                        "{line[29]}",
                        "{line[30]}",
                        "{line[31]}",
                        "{line[32]}",
                        "{line[33]}",
                        "{line[34]}",
                        "{line[35]}",
                        "{line[36]}",
                        "{line[37]}",
                        "{line[38]}",
                        "{line[39]}",
                        "{line[40]}",
                        "{line[41]}",
                        "{line[42]}",
                        "{line[43]}",
                        "{line[44]}",
                        null,
                        "{line[46]}",
                        "{line[47]}",
                        "{line[48]}",
                        "{line[49]}",
                        "{line[50]}",
                        "{line[51]}"
                    ) ON DUPLICATE KEY UPDATE XM = "{line[1]}",XMPY = "{line[2]}",XBDM = "{line[3]}",
                    XBMC = "{line[4]}",
                    CSRQ = "{line[5]}",
                    MZDM = "{line[6]}",
                    MZMC = "{line[7]}",
                    ZZMMDM = "{line[8]}",
                    ZZMMMC = "{line[9]}",
                    JGDM = "{line[10]}",
                    JGMC = "{line[11]}",
                    HYZKMC = "{line[12]}",
                    GBDM = "{line[13]}",
                    GBMC = "{line[14]}",
                    SFZHM = "{line[15]}",
                    XLDM = "{line[16]}",
                    XLMC = "{line[17]}",
                    SYDQM = "{line[18]}",
                    SYDQMC = "{line[19]}",
                    JTDZ = "{line[20]}",
                    EMAIL = "{line[21]}",
                    SJ = "{line[22]}",
                    PYFSDM = "{line[23]}",
                    PYFSMC = "{line[24]}",
                    YXDM = "{line[25]}",
                    YXMC = "{line[26]}",
                    BH = "{line[27]}",
                    ZYDM = "{line[28]}",
                    ZYMC = "{line[29]}",
                    DSGH = "{line[30]}",
                    DSXM = "{line[31]}",
                    DSGH2 = "{line[32]}",
                    DSXM2 = "{line[33]}",
                    LXBJ = "{line[34]}",
                    LXBJMC = "{line[35]}",
                    XQDM = "{line[36]}",
                    SJLY = "{line[37]}",
                    XJZTMX = "{line[38]}",
                    XJZTMXMC = "{line[39]}",
                    XZ = "{line[40]}",
                    XXFSDM = "{line[41]}",
                    XXFSMC = "{line[42]}",
                    SFLXS = "{line[43]}",
                    YJBYSJ = "{line[44]}",
                    DBRQ = null,
                    BYSJ = "{line[46]}",
                    RXNJ = "{line[47]}",
                    BYXWDM = "{line[48]}",
                    BYXWMC = "{line[49]}",
                    SCBJ = "{line[50]}",
                    SJC = "{line[51]}"
                    """
                elif line[46] == "(NULL)":
                    sql = f"""
                    insert into {target_tbl} (
                        XH,
                        XM,
                        XMPY,
                        XBDM,
                        XBMC,
                        CSRQ,
                        MZDM,
                        MZMC,
                        ZZMMDM,
                        ZZMMMC,
                        JGDM,
                        JGMC,
                        HYZKMC,
                        GBDM,
                        GBMC,
                        SFZHM,
                        XLDM,
                        XLMC,
                        SYDQM,
                        SYDQMC,
                        JTDZ,
                        EMAIL,
                        SJ,
                        PYFSDM,
                        PYFSMC,
                        YXDM,
                        YXMC,
                        BH,
                        ZYDM,
                        ZYMC,
                        DSGH,
                        DSXM,
                        DSGH2,
                        DSXM2,
                        LXBJ,
                        LXBJMC,
                        XQDM,
                        SJLY,
                        XJZTMX,
                        XJZTMXMC,
                        XZ,
                        XXFSDM,
                        XXFSMC,
                        SFLXS,
                        YJBYSJ,
                        DBRQ,
                        BYSJ,
                        RXNJ,
                        BYXWDM,
                        BYXWMC,
                        SCBJ,
                        SJC               
                    ) values (
                        "{line[0]}",
                        "{line[1]}",
                        "{line[2]}",
                        "{line[3]}",
                        "{line[4]}",
                        "{line[5]}",
                        "{line[6]}",
                        "{line[7]}",
                        "{line[8]}",
                        "{line[9]}",
                        "{line[10]}",
                        "{line[11]}",
                        "{line[12]}",
                        "{line[13]}",
                        "{line[14]}",
                        "{line[15]}",
                        "{line[16]}",
                        "{line[17]}",
                        "{line[18]}",
                        "{line[19]}",
                        "{line[20]}",
                        "{line[21]}",
                        "{line[22]}",
                        "{line[23]}",
                        "{line[24]}",
                        "{line[25]}",
                        "{line[26]}",
                        "{line[27]}",
                        "{line[28]}",
                        "{line[29]}",
                        "{line[30]}",
                        "{line[31]}",
                        "{line[32]}",
                        "{line[33]}",
                        "{line[34]}",
                        "{line[35]}",
                        "{line[36]}",
                        "{line[37]}",
                        "{line[38]}",
                        "{line[39]}",
                        "{line[40]}",
                        "{line[41]}",
                        "{line[42]}",
                        "{line[43]}",
                        "{line[44]}",
                        "{line[45]}",
                        null,
                        "{line[47]}",
                        "{line[48]}",
                        "{line[49]}",
                        "{line[50]}",
                        "{line[51]}"
                    ) ON DUPLICATE KEY UPDATE XM = "{line[1]}",XMPY = "{line[2]}",XBDM = "{line[3]}",
                    XBMC = "{line[4]}",
                    CSRQ = "{line[5]}",
                    MZDM = "{line[6]}",
                    MZMC = "{line[7]}",
                    ZZMMDM = "{line[8]}",
                    ZZMMMC = "{line[9]}",
                    JGDM = "{line[10]}",
                    JGMC = "{line[11]}",
                    HYZKMC = "{line[12]}",
                    GBDM = "{line[13]}",
                    GBMC = "{line[14]}",
                    SFZHM = "{line[15]}",
                    XLDM = "{line[16]}",
                    XLMC = "{line[17]}",
                    SYDQM = "{line[18]}",
                    SYDQMC = "{line[19]}",
                    JTDZ = "{line[20]}",
                    EMAIL = "{line[21]}",
                    SJ = "{line[22]}",
                    PYFSDM = "{line[23]}",
                    PYFSMC = "{line[24]}",
                    YXDM = "{line[25]}",
                    YXMC = "{line[26]}",
                    BH = "{line[27]}",
                    ZYDM = "{line[28]}",
                    ZYMC = "{line[29]}",
                    DSGH = "{line[30]}",
                    DSXM = "{line[31]}",
                    DSGH2 = "{line[32]}",
                    DSXM2 = "{line[33]}",
                    LXBJ = "{line[34]}",
                    LXBJMC = "{line[35]}",
                    XQDM = "{line[36]}",
                    SJLY = "{line[37]}",
                    XJZTMX = "{line[38]}",
                    XJZTMXMC = "{line[39]}",
                    XZ = "{line[40]}",
                    XXFSDM = "{line[41]}",
                    XXFSMC = "{line[42]}",
                    SFLXS = "{line[43]}",
                    YJBYSJ = "{line[44]}",
                    DBRQ = "{line[45]}",
                    BYSJ = null,
                    RXNJ = "{line[47]}",
                    BYXWDM = "{line[48]}",
                    BYXWMC = "{line[49]}",
                    SCBJ = "{line[50]}",
                    SJC = "{line[51]}"
                    """

                cursor.execute(sql)
                conn.commit()


# 校园卡信息upsert
def upsertDfTo_JACCOUNT_CAMPUS(principal: str, df, target_tbl: str) -> tuple or None:
    if settings.current_env == 'DEV':
        host = settings.HOST_LOCAL
        port = settings.PORT_LOCAL
        user = settings.USER_LOCAL
        pwd = settings.PWD_LOCAL
        db = 'test'
        module_logger.info('准备写入local test, JG_JBXX_CAMPUS_TEST')
    elif settings.current_env == 'PROD':
        if principal.lower() == 'zjgyy':
            host = settings.HOST_ZJGYY
            port = settings.PORT_ZJGYY
            user = settings.USER_ZJGYY
            pwd = settings.PWD_ZJGYY
            db = 'jsc'
        elif principal.lower() == 'zjls':
            host = settings.HOST_ZJLS
            port = settings.PORT_ZJLS
            user = settings.USER_ZJLS
            pwd = settings.PWD_ZJLS
            db = 'jsc'
            module_logger.info(f'准备写入{principal}, {target_tbl}')

    with MySQLdb.connect(host=host, port=port, user=user, passwd=pwd, db=db, connect_timeout=10,
                         charset='utf8') as conn:
        with conn.cursor() as cursor:
            for line in df.to_numpy().tolist():
                line = list(map(lambda x: '(NULL)' if x is None else x, line))
                sql = f"""
                insert into {target_tbl} (
                    JACCOUNT,
                    XM,                     
                    EXPIREDATE,                 
                    userid,                
                    RYLB,               
                    YXDM,                   
                    YXMC,               
                    SJC,        
                    SCBJ  
                ) values (
                    "{line[0]}",
                    "{line[1]}",
                    "{line[2]}",
                    "{line[3]}",
                    "{line[4]}",
                    "{line[5]}",
                    "{line[6]}",
                    "{line[7]}",
                    "{line[8]}"
                ) ON DUPLICATE KEY UPDATE XM = "{line[1]}",EXPIREDATE = "{line[2]}",userid = "{line[3]}",
                RYLB = "{line[4]}",
                YXDM = "{line[5]}",
                YXMC = "{line[6]}",
                SJC  = "{line[7]}",
                SCBJ = "{line[8]}"
                """
                cursor.execute(sql)
                conn.commit()

#更新卡信息
def upsertDfTo_Card_CAMPUS(principal: str, df, target_tbl: str) -> tuple or None:
    if settings.current_env == 'DEV':
        host = settings.HOST_LOCAL
        port = settings.PORT_LOCAL
        user = settings.USER_LOCAL
        pwd = settings.PWD_LOCAL
        db = 'test'
        module_logger.info('准备写入local test, JG_JBXX_CAMPUS')
    elif settings.current_env == 'PROD':
        if principal.lower() == 'zjgyy':
            host = settings.HOST_ZJGYY
            port = settings.PORT_ZJGYY
            user = settings.USER_ZJGYY
            pwd = settings.PWD_ZJGYY
            db = 'jsc'
        elif principal.lower() == 'zjls':
            host = settings.HOST_ZJLS
            port = settings.PORT_ZJLS
            user = settings.USER_ZJLS
            pwd = settings.PWD_ZJLS
            db = 'jsc'
            module_logger.info(f'准备写入{principal}, {target_tbl}')

    with MySQLdb.connect(host=host, port=port, user=user, passwd=pwd, db=db, connect_timeout=10,
                         charset='utf8') as conn:
        with conn.cursor() as cursor:
            for line in df.to_numpy().tolist():
                line = list(map(lambda x: '(NULL)' if x is None else x, line))
                sql = f"""
                insert into {target_tbl} (
                    ID,
                    XGH, 
                    XM,                                                       
                    BMDM,               
                    BMMC,   
                    KH,                  
                    YXQ,   
                    ZHBZ,     
                    UPDATE_TIME,
                    SFMC  
                ) values (
                    "{line[0]}",
                    "{line[1]}",
                    "{line[2]}",
                    "{line[3]}",
                    "{line[4]}",
                    "{line[5]}",
                    "{line[6]}",
                    "{line[7]}",
                    "{line[8]}",
                    "{line[9]}"
                ) ON DUPLICATE KEY UPDATE XGH = "{line[1]}",XM = "{line[2]}",BMDM = "{line[3]}",
                BMMC = "{line[4]}",
                KH   = "{line[5]}",
                YXQ  = "{line[6]}",
                ZHBZ = "{line[7]}",
                UPDATE_TIME = "{line[8]}",
                SFMC = "{line[9]}"
                """
                cursor.execute(sql)
                conn.commit()

