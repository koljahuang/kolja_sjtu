import logging
import pandas as pd
import traceback
from config import settings
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
from src.util.DtFormat import DtUtil


LOG_NAME = settings.LOG_NAME
module_logger = logging.getLogger(f'{LOG_NAME}')


def sync_gyy_students(zj_principal):

    if settings.current_env == 'PROD':
        from src.core.xdb_io import mysqlQuery, mysqlTwoQuery, sqlServerQuery, oraclQueryWithDDL, oraclQuery, upsertDfTo_XS_JBXX_CAMPUS
        # target_tbl = 'jsc.XS_JBXX_CAMPUS'
        target_tbl = 'jsc.XS_JBXX_CAMPUS_TEST'
    if settings.current_env == 'DEV':
        from src.core.ssh_db_io import mysqlQuery, mysqlTwoQuery, sqlServerQuery, oraclQueryWithDDL, oraclQuery
        from src.core.xdb_io import upsertDfTo_XS_JBXX_CAMPUS
        target_tbl = 'test.xs_jbxx_campus'

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

    target_dw, target_ry = mysqlTwoQuery(principal=zj_principal, db='jsc', sql1=sql_dw, sql2=sql_ry)
    synced_dw, synced_ry = mysqlTwoQuery(principal=zj_principal, db='jsc', 
                        sql1=f'select YXDM from TB_DW_CAMPUS where (TGRQ >= "{execution_date_15m_ago}" or TGRQ is null)', 
                        sql2=f'select XGH from TB_RY_CAMPUS where (TGSJ >= "{execution_date_15m_ago}" or TGSJ is null)')

    # e.g. "'32500','32600','63200'"
    target_dw_str = ''
    for i in map(lambda x: x[0], target_dw):
        target_dw_str = target_dw_str + "'" + i + "'" + ","
    target_dw_str = target_dw_str[:-1]    
    
    target_ry_str = ''
    for i in map(lambda x: x[0], target_ry):
        target_ry_str = target_ry_str + "'" + i + "'" + ","
    target_ry_str = target_ry_str[:-1]

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

    if target_ry_str and target_dw_str:
        sql = f'''
        select 
            jbk.XH AS XH,
            jbk.XM AS XM,
            jbk.XMPY AS XMPY,
            jbk.XBDM AS XBDM,
            xb.C_NAME AS XBMC,
            jbk.CSRQ AS CSRQ,
            jbk.MZDM AS MZDM,
            mz.MZMC AS MZMC,
            jbk.ZZMMDM AS ZZMMDM,
            zzmm.C_NAME AS ZZMMMC,
            jbk.JGDM AS JGDM,
            jg.MC AS JGMC,
            hyzk.mc AS HYZKMC,
            jbk.GBDM AS GBDM,
            gb.C_NAME AS GBMC,
            rtrim(jbk.SFZHM) AS SFZHM,
            jbk.XLDM AS XLDM,
            xl.mc AS XLMC,
            jbk.SYDQM AS SYDQM,
            syqdm.MC AS SYDQMC,
            jbk.JTDZ AS JTDZ,
            jbk.EMAIL AS EMAIL,
            jbk.SJ AS SJ,
            jbk.PYFSDM AS PYFSDM,
            pyfs.C_NAME AS PYFSMC,
            jbk.YXSH AS YXDM,
            yxdmk.yxmc AS YXMC,
            jbk.BH AS BH,
            jbk.ZYH AS ZYDM,
            zyb.C_NAME AS ZYMC,
            jbk.DSGH AS DSGH,
            jbk.DSXM AS DSXM,
            jbk.DSGH2 AS DSGH2,
            jbk.DSXM2 AS DSXM2,
            case when jbk.LXBJ = 0 and jbk.SJLY = '60300' or jbk.TJ = 'Y' and jbk.SJLY = '60200' then 'N' else 'Y' end AS LXBJ,
            case when jbk.LXBJ = 0 and jbk.SJLY = '60300' or jbk.TJ = 'Y' and jbk.SJLY = '60200' then '在校' else '不在校' end AS LXBJMC,jbk.XQDM AS XQDM,
            case when jbk.SJLY = '60300' then '研究生' else '本科生' end AS SJLY,
            jbk.XJZTMX AS XJZTMX,
            XJZT.C_NAME AS XJZTMXMC,
            jbk.XZ AS XZ,
            jbk.XXFSDM AS XXFSDM,
            XXFS.C_NAME AS XXFSMC,
            jbk.SFLXS AS SFLXS,
            mx.YJBYSJ AS YJBYSJ,
            mx.DBRQ AS DBRQ,
            mx.BYSJ AS BYSJ,
            lq.RXNJ AS RXNJ,
            '' AS BYXWDM,
            '' AS BYXWMC,
            jbk.SCBJ AS SCBJ,
            str_to_date(jbk.SJC,'%Y-%m-%d %H:%i:%S') AS SJC 
        from xs_xsjbk jbk 
        left join dm_xb xb on jbk.XBDM = xb.C_ID 
        left join DM_MZ mz on jbk.MZDM = mz.MZDM
        left join dm_zzmm zzmm on jbk.ZZMMDM = zzmm.C_ID
        left join DM_GB2260 jg on jbk.JGDM = jg.DM
        left join DM_GB4766 hyzk on jbk.HYZKDM = hyzk.dm
        left join dm_gjdqmc gb on jbk.GBDM = gb.C_ID
        left join DM_GB4658 xl on jbk.XLDM = xl.dm
        left join DM_GB2260 syqdm on jbk.JGDM = syqdm.DM
        left join DM_PYFS_ALL pyfs on jbk.PYFSDM = pyfs.C_ID and jbk.SJLY = pyfs.SJLY
        left join DM_YXDMK yxdmk on jbk.YXSH = yxdmk.yxdm
        left join DM_ZYB_ALL zyb on jbk.ZYH = zyb.C_ID and jbk.SJLY = zyb.SJLY
        left join DM_XJZT_ALL XJZT on jbk.XJZTMX = XJZT.C_ID and jbk.SJLY = XJZT.SJLY
        left join DM_XXFS_ALL XXFS on jbk.XXFSDM = XXFS.C_ID and jbk.SJLY = XXFS.SJLY
        left join XS_BY mx on jbk.XH = mx.XH
        left join xs_kslq lq on jbk.XH = lq.XH
        where (jbk.XH in ({target_ry_str}) or jbk.YXSH in ({target_dw_str}))
        '''
    elif target_ry_str:
        sql = f'''
        select 
            jbk.XH AS XH,
            jbk.XM AS XM,
            jbk.XMPY AS XMPY,
            jbk.XBDM AS XBDM,
            xb.C_NAME AS XBMC,
            jbk.CSRQ AS CSRQ,
            jbk.MZDM AS MZDM,
            mz.MZMC AS MZMC,
            jbk.ZZMMDM AS ZZMMDM,
            zzmm.C_NAME AS ZZMMMC,
            jbk.JGDM AS JGDM,
            jg.MC AS JGMC,
            hyzk.mc AS HYZKMC,
            jbk.GBDM AS GBDM,
            gb.C_NAME AS GBMC,
            rtrim(jbk.SFZHM) AS SFZHM,
            jbk.XLDM AS XLDM,
            xl.mc AS XLMC,
            jbk.SYDQM AS SYDQM,
            syqdm.MC AS SYDQMC,
            jbk.JTDZ AS JTDZ,
            jbk.EMAIL AS EMAIL,
            jbk.SJ AS SJ,
            jbk.PYFSDM AS PYFSDM,
            pyfs.C_NAME AS PYFSMC,
            jbk.YXSH AS YXDM,
            yxdmk.yxmc AS YXMC,
            jbk.BH AS BH,
            jbk.ZYH AS ZYDM,
            zyb.C_NAME AS ZYMC,
            jbk.DSGH AS DSGH,
            jbk.DSXM AS DSXM,
            jbk.DSGH2 AS DSGH2,
            jbk.DSXM2 AS DSXM2,
            case when jbk.LXBJ = 0 and jbk.SJLY = '60300' or jbk.TJ = 'Y' and jbk.SJLY = '60200' then 'N' else 'Y' end AS LXBJ,
            case when jbk.LXBJ = 0 and jbk.SJLY = '60300' or jbk.TJ = 'Y' and jbk.SJLY = '60200' then '在校' else '不在校' end AS LXBJMC,jbk.XQDM AS XQDM,
            case when jbk.SJLY = '60300' then '研究生' else '本科生' end AS SJLY,
            jbk.XJZTMX AS XJZTMX,
            XJZT.C_NAME AS XJZTMXMC,
            jbk.XZ AS XZ,
            jbk.XXFSDM AS XXFSDM,
            XXFS.C_NAME AS XXFSMC,
            jbk.SFLXS AS SFLXS,
            mx.YJBYSJ AS YJBYSJ,
            mx.DBRQ AS DBRQ,
            mx.BYSJ AS BYSJ,
            lq.RXNJ AS RXNJ,
            '' AS BYXWDM,
            '' AS BYXWMC,
            jbk.SCBJ AS SCBJ,
            str_to_date(jbk.SJC,'%Y-%m-%d %H:%i:%S') AS SJC 
        from xs_xsjbk jbk 
        left join dm_xb xb on jbk.XBDM = xb.C_ID 
        left join DM_MZ mz on jbk.MZDM = mz.MZDM
        left join dm_zzmm zzmm on jbk.ZZMMDM = zzmm.C_ID
        left join DM_GB2260 jg on jbk.JGDM = jg.DM
        left join DM_GB4766 hyzk on jbk.HYZKDM = hyzk.dm
        left join dm_gjdqmc gb on jbk.GBDM = gb.C_ID
        left join DM_GB4658 xl on jbk.XLDM = xl.dm
        left join DM_GB2260 syqdm on jbk.JGDM = syqdm.DM
        left join DM_PYFS_ALL pyfs on jbk.PYFSDM = pyfs.C_ID and jbk.SJLY = pyfs.SJLY
        left join DM_YXDMK yxdmk on jbk.YXSH = yxdmk.yxdm
        left join DM_ZYB_ALL zyb on jbk.ZYH = zyb.C_ID and jbk.SJLY = zyb.SJLY
        left join DM_XJZT_ALL XJZT on jbk.XJZTMX = XJZT.C_ID and jbk.SJLY = XJZT.SJLY
        left join DM_XXFS_ALL XXFS on jbk.XXFSDM = XXFS.C_ID and jbk.SJLY = XXFS.SJLY
        left join XS_BY mx on jbk.XH = mx.XH
        left join xs_kslq lq on jbk.XH = lq.XH
        where jbk.XH in ({target_ry_str})
        '''
    elif target_dw_str:
        sql = f'''
        select 
            jbk.XH AS XH,
            jbk.XM AS XM,
            jbk.XMPY AS XMPY,
            jbk.XBDM AS XBDM,
            xb.C_NAME AS XBMC,
            jbk.CSRQ AS CSRQ,
            jbk.MZDM AS MZDM,
            mz.MZMC AS MZMC,
            jbk.ZZMMDM AS ZZMMDM,
            zzmm.C_NAME AS ZZMMMC,
            jbk.JGDM AS JGDM,
            jg.MC AS JGMC,
            hyzk.mc AS HYZKMC,
            jbk.GBDM AS GBDM,
            gb.C_NAME AS GBMC,
            rtrim(jbk.SFZHM) AS SFZHM,
            jbk.XLDM AS XLDM,
            xl.mc AS XLMC,
            jbk.SYDQM AS SYDQM,
            syqdm.MC AS SYDQMC,
            jbk.JTDZ AS JTDZ,
            jbk.EMAIL AS EMAIL,
            jbk.SJ AS SJ,
            jbk.PYFSDM AS PYFSDM,
            pyfs.C_NAME AS PYFSMC,
            jbk.YXSH AS YXDM,
            yxdmk.yxmc AS YXMC,
            jbk.BH AS BH,
            jbk.ZYH AS ZYDM,
            zyb.C_NAME AS ZYMC,
            jbk.DSGH AS DSGH,
            jbk.DSXM AS DSXM,
            jbk.DSGH2 AS DSGH2,
            jbk.DSXM2 AS DSXM2,
            case when jbk.LXBJ = 0 and jbk.SJLY = '60300' or jbk.TJ = 'Y' and jbk.SJLY = '60200' then 'N' else 'Y' end AS LXBJ,
            case when jbk.LXBJ = 0 and jbk.SJLY = '60300' or jbk.TJ = 'Y' and jbk.SJLY = '60200' then '在校' else '不在校' end AS LXBJMC,jbk.XQDM AS XQDM,
            case when jbk.SJLY = '60300' then '研究生' else '本科生' end AS SJLY,
            jbk.XJZTMX AS XJZTMX,
            XJZT.C_NAME AS XJZTMXMC,
            jbk.XZ AS XZ,
            jbk.XXFSDM AS XXFSDM,
            XXFS.C_NAME AS XXFSMC,
            jbk.SFLXS AS SFLXS,
            mx.YJBYSJ AS YJBYSJ,
            mx.DBRQ AS DBRQ,
            mx.BYSJ AS BYSJ,
            lq.RXNJ AS RXNJ,
            '' AS BYXWDM,
            '' AS BYXWMC,
            jbk.SCBJ AS SCBJ,
            str_to_date(jbk.SJC,'%Y-%m-%d %H:%i:%S') AS SJC 
        from xs_xsjbk jbk 
        left join dm_xb xb on jbk.XBDM = xb.C_ID 
        left join DM_MZ mz on jbk.MZDM = mz.MZDM
        left join dm_zzmm zzmm on jbk.ZZMMDM = zzmm.C_ID
        left join DM_GB2260 jg on jbk.JGDM = jg.DM
        left join DM_GB4766 hyzk on jbk.HYZKDM = hyzk.dm
        left join dm_gjdqmc gb on jbk.GBDM = gb.C_ID
        left join DM_GB4658 xl on jbk.XLDM = xl.dm
        left join DM_GB2260 syqdm on jbk.JGDM = syqdm.DM
        left join DM_PYFS_ALL pyfs on jbk.PYFSDM = pyfs.C_ID and jbk.SJLY = pyfs.SJLY
        left join DM_YXDMK yxdmk on jbk.YXSH = yxdmk.yxdm
        left join DM_ZYB_ALL zyb on jbk.ZYH = zyb.C_ID and jbk.SJLY = zyb.SJLY
        left join DM_XJZT_ALL XJZT on jbk.XJZTMX = XJZT.C_ID and jbk.SJLY = XJZT.SJLY
        left join DM_XXFS_ALL XXFS on jbk.XXFSDM = XXFS.C_ID and jbk.SJLY = XXFS.SJLY
        left join XS_BY mx on jbk.XH = mx.XH
        left join xs_kslq lq on jbk.XH = lq.XH
        where jbk.YXSH in ({target_dw_str})
        '''
    else:
        sql = f'''
        select 
            jbk.XH AS XH,
            jbk.XM AS XM,
            jbk.XMPY AS XMPY,
            jbk.XBDM AS XBDM,
            xb.C_NAME AS XBMC,
            jbk.CSRQ AS CSRQ,
            jbk.MZDM AS MZDM,
            mz.MZMC AS MZMC,
            jbk.ZZMMDM AS ZZMMDM,
            zzmm.C_NAME AS ZZMMMC,
            jbk.JGDM AS JGDM,
            jg.MC AS JGMC,
            hyzk.mc AS HYZKMC,
            jbk.GBDM AS GBDM,
            gb.C_NAME AS GBMC,
            rtrim(jbk.SFZHM) AS SFZHM,
            jbk.XLDM AS XLDM,
            xl.mc AS XLMC,
            jbk.SYDQM AS SYDQM,
            syqdm.MC AS SYDQMC,
            jbk.JTDZ AS JTDZ,
            jbk.EMAIL AS EMAIL,
            jbk.SJ AS SJ,
            jbk.PYFSDM AS PYFSDM,
            pyfs.C_NAME AS PYFSMC,
            jbk.YXSH AS YXDM,
            yxdmk.yxmc AS YXMC,
            jbk.BH AS BH,
            jbk.ZYH AS ZYDM,
            zyb.C_NAME AS ZYMC,
            jbk.DSGH AS DSGH,
            jbk.DSXM AS DSXM,
            jbk.DSGH2 AS DSGH2,
            jbk.DSXM2 AS DSXM2,
            case when jbk.LXBJ = 0 and jbk.SJLY = '60300' or jbk.TJ = 'Y' and jbk.SJLY = '60200' then 'N' else 'Y' end AS LXBJ,
            case when jbk.LXBJ = 0 and jbk.SJLY = '60300' or jbk.TJ = 'Y' and jbk.SJLY = '60200' then '在校' else '不在校' end AS LXBJMC,jbk.XQDM AS XQDM,
            case when jbk.SJLY = '60300' then '研究生' else '本科生' end AS SJLY,
            jbk.XJZTMX AS XJZTMX,
            XJZT.C_NAME AS XJZTMXMC,
            jbk.XZ AS XZ,
            jbk.XXFSDM AS XXFSDM,
            XXFS.C_NAME AS XXFSMC,
            jbk.SFLXS AS SFLXS,
            mx.YJBYSJ AS YJBYSJ,
            mx.DBRQ AS DBRQ,
            mx.BYSJ AS BYSJ,
            lq.RXNJ AS RXNJ,
            '' AS BYXWDM,
            '' AS BYXWMC,
            jbk.SCBJ AS SCBJ,
            str_to_date(jbk.SJC,'%Y-%m-%d %H:%i:%S') AS SJC 
        from xs_xsjbk jbk 
        left join dm_xb xb on jbk.XBDM = xb.C_ID 
        left join DM_MZ mz on jbk.MZDM = mz.MZDM
        left join dm_zzmm zzmm on jbk.ZZMMDM = zzmm.C_ID
        left join DM_GB2260 jg on jbk.JGDM = jg.DM
        left join DM_GB4766 hyzk on jbk.HYZKDM = hyzk.dm
        left join dm_gjdqmc gb on jbk.GBDM = gb.C_ID
        left join DM_GB4658 xl on jbk.XLDM = xl.dm
        left join DM_GB2260 syqdm on jbk.JGDM = syqdm.DM
        left join DM_PYFS_ALL pyfs on jbk.PYFSDM = pyfs.C_ID and jbk.SJLY = pyfs.SJLY
        left join DM_YXDMK yxdmk on jbk.YXSH = yxdmk.yxdm
        left join DM_ZYB_ALL zyb on jbk.ZYH = zyb.C_ID and jbk.SJLY = zyb.SJLY
        left join DM_XJZT_ALL XJZT on jbk.XJZTMX = XJZT.C_ID and jbk.SJLY = XJZT.SJLY
        left join DM_XXFS_ALL XXFS on jbk.XXFSDM = XXFS.C_ID and jbk.SJLY = XXFS.SJLY
        left join XS_BY mx on jbk.XH = mx.XH
        left join xs_kslq lq on jbk.XH = lq.XH
        where (jbk.XH in ({synced_ry_str}) or jbk.YXSH in ({synced_dw_str}))
        '''

    if sql:
        try:
            res = mysqlQuery('sa', 'sams_factdata', sql)
        except Exception as e:
            traceback.print_exc()
        inc_pdf = pd.DataFrame(res, columns=['XH','XM','XMPY','XBDM','XBMC',
                                    'CSRQ','MZDM','MZMC','ZZMMDM','ZZMMMC',
                                    'JGDM','JGMC','HYZKMC','GBDM','GBMC',
                                    'SFZHM','XLDM','XLMC','SYDQM','SYDQMC',
                                    'JTDZ','EMAIL','SJ','PYFSDM','PYFSMC',
                                    'YXDM','YXMC','BH','ZYDM','ZYMC',
                                    'DSGH','DSXM','DSGH2','DSXM2','LXBJ',
                                    'LXBJMC','XQDM','SJLY','XJZTMX','XJZTMXMC',
                                    'XZ','XXFSDM','XXFSMC','SFLXS','YJBYSJ',
                                    'DBRQ','BYSJ','RXNJ','BYXWDM','BYXWMC',
                                    'SCBJ','SJC' ])
        module_logger.debug(inc_pdf)
        
        # TODO sink inc to ZJ
        if inc_pdf.shape[0] > 0:
            module_logger.info(f'准备写入张江高研院【{execution_date_15m_ago},{execution_date}）增量数据 ---> XS_JBXX_CAMPUS')
            upsertDfTo_XS_JBXX_CAMPUS(zj_principal, inc_pdf, target_tbl)
            del inc_pdf

    
    # At the sametime, Query Incremental data from SA
    # If has_new = True. Push Incremental data of SA to ZJ
    if is_init:
        '''
        It is unnecessary to judge whether JBXX in oracle has update information for the first time
        '''
        pass
    else:
        '''
        Query incremental data in SHAREDB.JG_JBXX.
        Query full updated data, if there is incremental data in the last 15 minutes.
        '''        
        sql += f"""and str_to_date(jbk.SJC,'%Y-%m-%d %H:%i:%S') >= '{execution_date_15m_ago}'
        and str_to_date(jbk.SJC,'%Y-%m-%d %H:%i:%S') < '{execution_date}'"""
        
        res = mysqlQuery('sa', 'sams_factdata', sql)
        module_logger.info(f'sa 更新了 {len(res)} 条数据')
        module_logger.info(res)

        if res:
            inc_sa_pdf = pd.DataFrame(res, columns=['XH','XM','XMPY','XBDM','XBMC',
                                    'CSRQ','MZDM','MZMC','ZZMMDM','ZZMMMC',
                                    'JGDM','JGMC','HYZKMC','GBDM','GBMC',
                                    'SFZHM','XLDM','XLMC','SYDQM','SYDQMC',
                                    'JTDZ','EMAIL','SJ','PYFSDM','PYFSMC',
                                    'YXDM','YXMC','BH','ZYDM','ZYMC',
                                    'DSGH','DSXM','DSGH2','DSXM2','LXBJ',
                                    'LXBJMC','XQDM','SJLY','XJZTMX','XJZTMXMC',
                                    'XZ','XXFSDM','XXFSMC','SFLXS','YJBYSJ',
                                    'DBRQ','BYSJ','RXNJ','BYXWDM','BYXWMC',
                                    'SCBJ','SJC' ])
            module_logger.debug(inc_sa_pdf)

            if inc_sa_pdf.shape[0] > 0:
                module_logger.info('准备写入张江高研院当前全量数据 ---> JG_JBXX_CAMPUS')
                upsertDfTo_XS_JBXX_CAMPUS(zj_principal, inc_sa_pdf, target_tbl)
