import logging
import pandas as pd
from config import settings
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
from src.util.DtFormat import DtUtil


LOG_NAME = settings.LOG_NAME
module_logger = logging.getLogger(f'{LOG_NAME}')

def sync_zj_card(dwmc: str)->None:

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
    获取校园卡信息
    '''
    Card_sql = f'''
       SELECT    ID                                         
             ,TRIM(AUTH_CARD.XGH) XGH                                
             ,Auth_CARD.XM                                           
     	    ,NVL(VDB2.C_ID,VDB4.C_ID) AS COLLEGE_CODE               
      ,NVL(VDB2.C_NAME,VDB4.C_NAME) AS COLLEGE_NAME           
      ,KH                                        
      ,YXQ                                                    
      ,ZHBZ                                                   
      ,Auth_CARD.sjc                                          
      ,SFLB.C_name AS SFMCCARD                                
        FROM 
        (SELECT a.*,row_number() over (partitiON by a.XGH order by a.id desc) rom
            FROM Auth_CARD a) Auth_CARD
        LEFT JOIN XS_XSJBK A ON trim(Auth_CARD.XGH)= A.XH
        LEFT JOIN standcode.dm_bm sdb1 ON A.YXSH=sdb1.C_ID
        LEFT JOIN XS_BY ON A.XH=XS_BY.XH
        LEFT JOIN standcode.dm_xb B ON A.Xbdm=B.c_Id
        LEFT JOIN standcode.dm_gjdqmc G ON trim(A.Gbdm)=G.C_ID
        LEFT JOIN v_dm_bm vdb1 ON A.glyx=vdb1.C_ID
        LEFT JOIN v_dm_bm vdb2 ON vdb1.PARENT1_C_ID=vdb2.C_ID
        LEFT JOIN STANDCODE.DM_CARD_SFLB SFLB ON Auth_CARD.pid=SFLB.c_id
        LEFT JOIN JG_JBXX JG ON trim(Auth_CARD.XGH)= JG.gh
        LEFT JOIN standcode.dm_bm sdm2 ON JG.yxdm=sdm2.C_ID
        LEFT JOIN v_dm_bm vdb3 ON JG.yxdm=vdb3.C_ID
        LEFT JOIN v_dm_bm vdb4 ON vdb3.PARENT1_C_ID=vdb4.C_ID
        WHERE rom=1 and (TRIM(AUTH_CARD.XGH) in ({target_ry_str}) 
        or NVL(VDB2.C_ID,VDB4.C_ID)  in ({target_dw_str}))
        '''

    res_card = oraclQuery(principal='gxk', sql=Card_sql)
    Card_pdf = pd.DataFrame(res_card, columns=['ID', 'XGH', 'XM', 'BMDM',
                                               'BMMC', 'KH', 'YXQ','ZHBZ', 'UPDATE_TIME', 'SFMC'])
    module_logger.debug(Card_pdf)

    Card_add_attrs = f'''
        select
            a.*
        from Card_pdf a
        where a.XGH in ({target_ry_str}) 
        or a.BMDM in ({target_dw_str})
        '''
    Card_add_attrs_pdf = sqldf(Card_add_attrs)

    # TODO sink Card to ZJ
    if Card_add_attrs_pdf.shape[0] > 0:
        module_logger.info(f'准备写入{dwmc}({execution_date_15m_ago},{execution_date}）增量数据 ---> Card_CAMPUS')
        upsertDfTo_Card_CAMPUS(dwmc, Card_add_attrs_pdf, target_card)
        del Card_add_attrs_pdf

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
        from  (SELECT     TO_CHAR(ID) ID                                         --ID
	   		 ,TRIM(AUTH_CARD.XGH) XGH                                --学工号
	       		 ,Auth_CARD.XM                                           --姓名
	    		 ,NVL(VDB2.C_ID,VDB4.C_ID) AS COLLEGE_CODE               --院系代码
		   	 ,NVL(VDB2.C_NAME,VDB4.C_NAME) AS COLLEGE_NAME           --院系名称
		   	 ,TO_CHAR(KH) KH                                         --卡号
		   	 ,YXQ                                                    --有效期
		   	 ,ZHBZ                                                   --账户标志
		   	 ,Auth_CARD.sjc                                          --时间戳
		   	 ,SFLB.C_name AS SFMCCARD                                --身份类别
		FROM 
		(SELECT a.*,row_number() over (partition by a.XGH order by a.id desc) rom
 		FROM Auth_CARD a) Auth_CARD
		--学生相关
		LEFT JOIN STANDCODE.DM_CARD_SFLB SFLB ON Auth_CARD.pid=SFLB.c_id
		LEFT JOIN XS_XSJBK A ON trim(Auth_CARD.XGH)= A.XH
		LEFT JOIN standcode.dm_bm sdb1 ON A.YXSH=sdb1.C_ID
		LEFT JOIN XS_BY ON A.XH=XS_BY.XH
		LEFT JOIN standcode.dm_xb B ON A.Xbdm=B.c_Id
		LEFT JOIN standcode.dm_gjdqmc G ON trim(A.Gbdm)=G.C_ID
		LEFT JOIN v_dm_bm vdb1 ON A.glyx=vdb1.C_ID
		LEFT JOIN v_dm_bm vdb2 ON vdb1.PARENT1_C_ID=vdb2.C_ID
		--教职工相关
		LEFT JOIN JG_JBXX JG ON trim(Auth_CARD.XGH)= JG.gh
		LEFT JOIN standcode.dm_bm sdm2 ON JG.yxdm=sdm2.C_ID
		LEFT JOIN v_dm_bm vdb3 ON JG.yxdm=vdb3.C_ID
		LEFT JOIN v_dm_bm vdb4 ON vdb3.PARENT1_C_ID=vdb4.C_ID
		--LEFT JOIN STANDCODE.DM_CARD_SFLB  SFLB2 ON Auth_CARD.pid=sflb2.c_id
		WHERE rom=1)
        where sjc >= '{execution_date_15m_ago}'
        and sjc < '{execution_date}'
        and COLLEGE_CODE in ('32500','32600','63200','42600') """
        res = oraclQueryWithDDL(principal='gxk',
                                preDDL="alter session set nls_timestamp_format = 'YYYY-MM-DD HH24:MI:SS.FF'", sql=sql)
        module_logger.info(f'Auth_CARD 更新了 {len(res)} 条数据')
        module_logger.info(res)

        if res:
            # 如果共享库identity_auth_account有更新，则全量推一遍数据
            all_sql = f'''
            select
                a.*
            from Card_pdf a
            where a.XGH in ({synced_ry_str}) 
            or a.BMDM in ({synced_dw_str})
            '''
            all_df = sqldf(all_sql)
            module_logger.debug('all_df:')
            module_logger.debug(f'\n{all_df}')

            if all_df.shape[0] > 0:
                module_logger.info(f'准备写入{dwmc}当前全量数据 ---> CARD_CAMOUS_test')
                upsertDfTo_Card_CAMPUS(dwmc, all_df, target_card)




















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
