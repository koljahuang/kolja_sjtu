import logging
import pandas as pd
from config import settings
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
from src.util.DtFormat import DtUtil


LOG_NAME = settings.LOG_NAME
module_logger = logging.getLogger(f'{LOG_NAME}')

def sync_zj_account(dwmc: str)->None:

    if settings.current_env == 'PROD':
        from src.core.xdb_io import mysqlQuery, mysqlTwoQuery, sqlServerQuery, oraclQueryWithDDL, oraclQuery, upsertDfTo_JACCOUNT_CAMPUS, upsertDfTo_Card_CAMPUS
        # target_tbl = 'jsc.JG_JBXX_CAMPUS'
        target_tbl = 'jsc.JACCOUNT_CAMPUS_test'
        target_card = 'jsc.CARD_CAMPUS_test'
    if settings.current_env == 'DEV':
        from src.core.ssh_db_io import mysqlQuery, mysqlTwoQuery, sqlServerQuery, oraclQueryWithDDL, oraclQuery
        from src.core.xdb_io import upsertDfTo_JG_JBXX_CAMPUS
        target_tbl = 'test.jg_jbxx_campus'


    CMD_ARGS = settings.CMD_ARGS
    module_logger.debug(CMD_ARGS)
    is_init = CMD_ARGS['is_init']
    execution_date = CMD_ARGS['execution_date']
    execution_date_15m_ago = DtUtil.n_minutes_ago(execution_date, 15, '%Y-%m-%d %X', '%Y-%m-%d %X')
   
    # The scanning range of incremental data is the last 15 minutes
    if is_init:
        '''
        If is_init is True. It has nothing to do with batch processing.
        Select all the people and pull all the their infos from src
        '''
        sql_dw = f'''select YXDM from TB_DW_CAMPUS where (TGRQ >= "{execution_date_15m_ago}" or TGRQ is null)'''
    else:
        # sql_dw = f'''drop table if exists target_dw; create table target_dw as 
        # select YXDM 
        # from TB_DW_CAMPUS 
        # where GXSJC >= "{execution_date_15m_ago}"
        # and GXSJC < "{execution_date}"'''
        sql_dw = f''' 
        select YXDM 
        from TB_DW_CAMPUS 
        where GXSJC >= "{execution_date_15m_ago}"
        and GXSJC < "{execution_date}"
        and (TGRQ >= "{execution_date_15m_ago}" or TGRQ is null)'''

    if is_init:
        sql_ry = f'''select XGH from TB_RY_CAMPUS where (TGSJ >= "{execution_date_15m_ago}" or TGSJ is null)'''
    else:
        # sql_ry = f'''drop table if exists target_ry; create table target_ry as
        # select XGH
        # from TB_RY_CAMPUS 
        # where GXSJC >= "{execution_date_15m_ago}"
        # and GXSJC < "{execution_date}"'''
        sql_ry = f'''
        select XGH
        from TB_RY_CAMPUS 
        where GXSJC >= "{execution_date_15m_ago}"
        and GXSJC < "{execution_date}"
        and (TGSJ >= "{execution_date_15m_ago}" or TGSJ is null)'''

    target_dw, target_ry = mysqlTwoQuery(principal=dwmc, db='jsc', sql1=sql_dw, sql2=sql_ry)
    synced_dw, synced_ry = mysqlTwoQuery(principal=dwmc, db='jsc',
                        sql1=f'select YXDM from TB_DW_CAMPUS where (TGRQ >= "{execution_date_15m_ago}" or TGRQ is null)', 
                        sql2=f'select XGH from TB_RY_CAMPUS where (TGSJ >= "{execution_date_15m_ago}" or TGSJ is null)')

    # e.g. "'32500','32600','63200'"
    target_dw_str = ''
    for i in map(lambda x: x[0], target_dw):
        target_dw_str = target_dw_str + "'" + i + "'" + ","
    target_dw_str = target_dw_str[:-1]    

    #Oracle的in语法不能识别()，新增判断逻辑当target_dw_str为空，赋值''
    if not target_dw_str:
        target_dw_str="\'\'"

    target_ry_str = ''
    for i in map(lambda x: x[0], target_ry):
        target_ry_str = target_ry_str + "'" + i + "'" + ","
    target_ry_str = target_ry_str[:-1]

    if not target_ry_str:
        target_ry_str="\'\'"

    module_logger.info(f'涉及{len(target_dw)}家单位（院系所有人员）需要更新')
    module_logger.info(f'涉及{len(target_ry)}人（指定人员）需要更新')

    synced_dw_str = ''
    for i in map(lambda x: x[0], synced_dw):
        synced_dw_str = synced_dw_str + "'" + i + "'" + ","
    synced_dw_str = synced_dw_str[:-1] 

    synced_ry_str = ''
    for i in map(lambda x: x[0], synced_ry):
        synced_ry_str = synced_ry_str + "'" + i + "'" + ","
    synced_ry_str = synced_ry_str[:-1] 

    # TODO acquire df from oracle or sqlserver which to replace tables in SA
    '''
    获取Jaccount信息
    '''
    Jaccount_sql = f'''
	SELECT    ACCOUNT_NO as JACCOUNT
      		 ,a.display_name as xm 
      		 ,EXPIRE_DATE 
      		 ,USER_ID
      		 ,sds.C_NAME  
      		 ,a.p_organize_id as yxdm
      		 ,a.P_ORGANIZE_NAME  as yxmc
      		 ,a.sjc
      		 ,a.scbj 
	FROM  identity_auth_account a 
	LEFT JOIN standcode.dm_sflb sds 
	ON a.USER_STYLE = sds.c_id
	WHERE USER_ID in ({target_ry_str}) or a.p_organize_id in ({target_dw_str})
    '''

    res = oraclQuery(principal = 'gxk', sql = Jaccount_sql)
    Jaccount_pdf = pd.DataFrame(res, columns=['JACCOUNT', 'XM', 'EXPIREDATE', 'userid', 'RYLB',
                                              'YXDM', 'YXMC','SJC', 'SCBJ'])
    module_logger.debug(Jaccount_pdf)

    Jaccount_add_attrs = f'''
    select
        a.*
    from Jaccount_pdf a
    '''
    Jaccount_add_attrs_pdf = sqldf(Jaccount_add_attrs)

    # TODO sink inc to ZJ
    if Jaccount_add_attrs_pdf.shape[0] > 0:
        module_logger.info(f'准备写入{dwmc}({execution_date_15m_ago},{execution_date}）增量数据 ---> Jaccount_CAMPUS_test')
        upsertDfTo_JACCOUNT_CAMPUS(dwmc, Jaccount_add_attrs_pdf, target_tbl)
        del Jaccount_add_attrs_pdf
    

    # At the sametime, Query Incremental data from GXK
    # If has_new = True. Push all data to ZJ
    if is_init:
        '''
        It is unnecessary to judge whether identity_auth_account in oracle has update information for the first time
        '''
        pass
    else:
        '''
        Query incremental data in SHAREDB.identity_auth_account.
        Query full updated data, if there is incremental data in the last 15 minutes.
        '''        
        sql = f"""select 
            sjc 
        from identity_auth_account
        where sjc >= '{execution_date_15m_ago}'
        and sjc < '{execution_date}' and P_ORGANIZE_ID IN ('32500','32600','63200')"""
        res = oraclQueryWithDDL(principal='gxk', preDDL="alter session set nls_timestamp_format = 'YYYY-MM-DD HH24:MI:SS.FF'", sql=sql)
        module_logger.info(f'identity_auth_account 更新了 {len(res)} 条数据')
        module_logger.info(res)

        if res:
            # 如果共享库identity_auth_account有更新，则全量推一遍数据
            all_sql = f'''
            select
                a.*
            from Jaccount_pdf a
            where a.userid in ({synced_ry_str}) 
            or a.YXDM in ({synced_dw_str})
            '''
            all_df = sqldf(all_sql)
            module_logger.debug('all_df:')
            module_logger.debug(f'\n{all_df}')

            if all_df.shape[0] > 0:
                module_logger.info(f'准备写入{dwmc}当前全量数据 ---> Jaccount_CAMPUS')
                upsertDfTo_JACCOUNT_CAMPUS(dwmc, all_df, target_tbl)
    




    # # 开发过程验证脚本
    # # tmp = sqldf('''
    # # select 
  	# # 	*
    # # from 
    # #     (
    # #         select '001' uid, 'aa' schoolname, 2 educationdegcode
    # #         union 
    # #         select '001' uid, 'dd' schoolname, 1 educationdegcode
    # #         union 
    # #         select '002' uid, 'bb' schoolname, 1 educationdegcode

    # #     ) t
    # # ''')
    # # tmp = tmp.groupby('uid', group_keys=False).apply(lambda x: x.sort_values('educationdegcode', ascending=True))
    # # print(tmp)
    # # tmp = tmp.groupby('uid', as_index=False).apply(lambda x: ','.join(x.schoolname))
    # # tmp.columns=['a','b']
    # # tmp['c'] = tmp['b'].str.split(',')[0]
    # # print(tmp)
