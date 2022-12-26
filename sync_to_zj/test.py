from src.core.ssh_db_io import *
import pymysql
import pandas as pd

# sa view: V_JG_JBXX_CAMPUS
    # extends sams.hr_basic, sams_data.teacher_info_all_adl
        # from dbo.LSRYK
# write to jsc.JG_JBXX_CAMPUS
# read from jsc.TB_DW_CAMPUS
# read from jsc.TB_RY_CAMPUS

# print(sqlServerQuery('rsc', 'dbo', 'select 1'))
# print(sqlServerQuery('sa', 'sams_bak', 'show tables'))
# print(oraclQuery('gxk', 'sharedb', 'select * from v_dm_bm'))


# oracle query
from sshtunnel import SSHTunnelForwarder
import oracledb
ssh_host='10.118.1.192'
ssh_user='root'
ssh_pwd='zjycy20dnl-025F'
host='10.120.64.12'
port=1521
user='sharedb', 
password='sjtu_nor'
service_name = 'coredb'
oracledb.init_oracle_client('/Users/kolja/opt/module/instantclient_19_8')
with SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_user,
        ssh_password=ssh_pwd,
        remote_bind_address=(host, port)
    ) as tunnel:
        module_logger.debug('ssh succ...')
        host = tunnel.local_bind_address[0]
        port = tunnel.local_bind_address[1]
        print(host, port)
        dsn = f'%s/{password}@{host}:{port}/{service_name}'%(user)
        with oracledb.connect(dsn) as conn:
                with conn.cursor() as cursor:
                    res = cursor.execute('select * from JG_JBXX ')
                    print(res.fetchone())







