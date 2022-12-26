import logging
import pandas as pd
from config import settings
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
from src.util.DtFormat import DtUtil


LOG_NAME = settings.LOG_NAME
module_logger = logging.getLogger(f'{LOG_NAME}')

def sync_gyy_teachers(zj_principal):

    if settings.current_env == 'PROD':
        from src.core.xdb_io import mysqlQuery, mysqlTwoQuery, sqlServerQuery, oraclQueryWithDDL, oraclQuery, upsertDfTo_JG_JBXX_CAMPUS
        # target_tbl = 'jsc.JG_JBXX_CAMPUS'
        target_tbl = 'jsc.JG_JBXX_CAMPUS_TEST'
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

    # TODO acquire df from oracle or sqlserver which to replace tables in SA
    '''
    replace: sams.hr_basic
    checked: select * from sams.hr_basic where scbj = 'N' and usertype = 'S'
    '''
    hr_basic_S_sql = '''
    select 
        USERID,
        NAME_CERT,
        GENDER,
        BIRTHDATE,
        IDCARDCODE,
        NATION,
        NATIONCODE,
        case when TELEPHONE is null then sj else TELEPHONE end TELEPHONE,
        JACCOUNT,
        FACULTY_AD,
        FACULTYCODE_AD,
        SUPERIORFACULTY_AD,
        SUPERIORFACULTYCODE_AD,
        UPDATE_TIMESTAMP,
        case when JOBSTATUS = '9' then '在职' 
            when JOBSTATUS ='99' then '返聘' 
            when JOBSTATUS ='4' then '离职' 
            when JOBSTATUS ='2' then '离退休' 
            else '其它' end RYZT,
        jzgmblc,
        gwmc,
        bzmc,
        print_mc
    from 
        (
            select 
                row_number() over (partition by User_Id order by BB.expire_date desc) rom,
                BB.Account_No jaccount,BB.User_Id,BB.TELEPHONE,AA.*,
                greatest(nvl(BB.SJC,to_date('1','yyyy')),nvl(AA.sjc,to_date('1','yyyy'))) as update_timestamp
            from  
                (
                    select 
                        A.gh userid,
                        A.xm name_cert,A.HYPY name_py,
                        null name_native,
                        A.gh staffno,
                        B.C_NAME gender,
                        A.xbm gendercode,
                        case when A.txfp='退休返聘（校内）' and A.txfp_pyjsrq>=to_char(sysdate,'yyyy.mm.dd') then N.c_name  when  A.Yxdm_Academia is not null then C.C_name else C1.c_name end   faculty,
                        case when A.txfp='退休返聘（校内）' and A.txfp_pyjsrq>=to_char(sysdate,'yyyy.mm.dd') then txfp_yxdm when  A.Yxdm_Academia is not null then A.Yxdm_Academia else A.yxdm  end   facultycode,
                        case when A.txfp='退休返聘（校内）' and A.txfp_pyjsrq>=to_char(sysdate,'yyyy.mm.dd') then O.c_name  when  A.Yxdm_Academia is not null then D.C_name else C1.c_name end   superiorfaculty,
                        case when A.txfp='退休返聘（校内）' and A.txfp_pyjsrq>=to_char(sysdate,'yyyy.mm.dd') then O.c_id when  A.Yxdm_Academia is not null then D.c_id else D1.c_id  end   superiorfacultycode,
                        A.csrq Birthdate,A.sfzh idcardcode,E.C_ID idcardtype,E.C_ID idcardtypecode,
                        F.C_NAME marrige,A.Hyzkm marrigecode,G.C_NAME nation,A.GJM nationcode,H.C_NAME nativeplace,A.JGM nativeplacecode,I.C_NAME regresidence,A.HKSZD regresidencecode,
                        J.C_name race,A.MZM racecode,A.Gzny workym,
                        A.lxny schoolym,
                        A.jzglbm,L.c_Short_Name facultycategory,A.rylbm hirecode,M.c_Name hire,
                        A.SJC ,
                        A.Ryztm jobstatus,
                        A.scbj,
                        A.Sj,
                        case when A.txfp='退休返聘（校内）' and A.txfp_pyjsrq>=to_char(sysdate,'yyyy.mm.dd') then N.c_name  else C1.c_name end   faculty_ad,
                        case when A.txfp='退休返聘（校内）' and A.txfp_pyjsrq>=to_char(sysdate,'yyyy.mm.dd') then txfp_yxdm  else A.yxdm  end   facultycode_ad,
                        case when A.txfp='退休返聘（校内）' and A.txfp_pyjsrq>=to_char(sysdate,'yyyy.mm.dd') then O.c_name  else D1.c_name end   superiorfaculty_ad,
                        case when A.txfp='退休返聘（校内）' and A.txfp_pyjsrq>=to_char(sysdate,'yyyy.mm.dd') then O.c_id    else D1.c_id  end   superiorfacultycode_ad,
                        L.C_NAME jzgmblc,
                        CC.C_NAME bzmc,
                        EE.C_NAME gwmc,
                        BB.print_mc
                    from jg_jbxx A
                    left join appcode.RS_CODE_014 B on trim(A.Xbm)=B.C_ID
                    left join v_dm_bm C on A.Yxdm_Academia=C.C_ID
                    left join v_dm_bm D on C.PARENT1_C_ID=D.C_ID
                    left join v_dm_bm C1 on A.yxdm=C1.C_ID
                    left join v_dm_bm D1 on C1.PARENT1_C_ID=D1.C_ID
                    left join standcode.dm_sfzjlb E on trim(A.SFZJLXM) =E.C_name
                    left join standcode.dm_hyzk F on trim(A.Hyzkm) =F.C_ID
                    left join standcode.dm_gjdqmc G on trim(A.gjm)=G.C_ID
                    left join standcode.DM_XZQH H on trim(A.jgm)=H.C_ID
                    left join standcode.DM_XZQH I on trim(A.HKSZD)=I.C_ID
                    left join standcode.DM_ZGMZMC J on trim(A.MZM)=J.C_ID
                    left join standcode.dm_jzglb_gw L on trim(A.Jzglbm)=L.C_ID
                    left join standcode.dm_rylbm M on trim(A.rylbm)=M.C_ID
                    left join v_dm_bm N on A.txfp_yxdm=N.C_ID
                    left join v_dm_bm O on N.PARENT1_C_ID=O.C_ID
                    left join standcode.dm_bzlb_gw CC on trim(A.BZM) = CC.C_ID
                    left join standcode.dm_bzlb_gw EE on trim(A.GWM) = EE.C_ID
                    left join standcode.dm_gb8561 BB on substr(trim(A.przwm),1,3) = BB.C_ID
                    where A.SCBJ = 'N'
                )AA
            left join 
                (
                    select 
                        B.ACCOUNT_NO,B.TELEPHONE,B.expire_date,A.user_id,B.Sjc,
                        row_number() over (partition by A.USER_ID ORDER BY B.SJC desc) rnk 
                    from identity_auth_relation A
                    inner join identity_auth_account B 
                    on A.guid=B.guid and A.user_id=B.user_id and B.guid is not null  and A.user_style in ('faculty','postphd','yxy','outsourcing','green','external_teacher') 
                    where A.del_mark='N' 
                    and A.scbj = 'N'
                    and B.Del_Mark='N' 
                    and B.Account_Status='正常'
                ) BB
            on AA.userid = BB.USER_ID AND BB.rnk = 1 
        )
    where rom<=1 or user_id is null
    '''

    res = oraclQuery(principal='gxk', sql=hr_basic_S_sql)
    hr_basic_S_pdf = pd.DataFrame(res, columns=['USERID','NAME_CERT','GENDER', 'BIRTHDATE','IDCARDCODE',
                                              'NATION','NATIONCODE','TELEPHONE','JACCOUNT','FACULTY',
                                              'FACULTYCODE','SUPERIORFACULTY','SUPERIORFACULTYCODE',
                                              'UPDATE_TIMESTAMP','RYZT', 'jzgmblc', 'gwmc', 'bzmc', 'print_mc'])
    module_logger.debug(hr_basic_S_pdf)


    if settings.current_env == 'DEV':
        hr_basic_SA_sql = '''
        select userid from sams.hr_basic where scbj = 'N' and usertype = 'S'
        '''
        res = mysqlQuery('sa', 'sams', hr_basic_SA_sql)
        hr_basic_SA_pdf = pd.DataFrame(res, columns=['USERID'])

        diff_sql = '''
        select sa.userid sa_uid, ora.userid ora_uid
        from hr_basic_SA_pdf sa
        left join hr_basic_S_pdf ora
        on sa.userid = ora.userid
        where ora.userid is null

        union all

        select sa.userid, ora.userid from 
        hr_basic_S_pdf ora
        left join hr_basic_SA_pdf sa
        on sa.userid = ora.userid
        where sa.userid is null
        '''
        diff_pdf = sqldf(diff_sql)
        module_logger.warning('============== hr_basic 业务库 和 SA 数据存在差异 ==============')
        module_logger.warning(f'\n{diff_pdf}')


    '''
    replace: sams.hr_party
    checked: select * from sams.hr_party where scbj = 'N'
             [14426 rows x 8 columns]
    '''
    hr_party_sql = '''
    SELECT
        *
    FROM
        (
            SELECT     
                t.gh, 
                t.xh, 
                t.dpdm, 
                t.cjny, 
                t.tcny, 
                t.tcyy, 
                (CASE t .Qr WHEN 0 THEN '修改中' WHEN 1 THEN '确认中' WHEN 2 THEN '已确认' END) qr, 
                dp.mc dpmc
            FROM 
                dbo.DPK AS t 
            LEFT OUTER JOIN
                dbo.DM_GB4763 AS dp 
            ON t.dpdm = dp.dm
            WHERE  t.xh >= 0
            and (scbj is null or scbj = 'N')
        ) tmp
    WHERE qr = '已确认'
    '''
    res = sqlServerQuery(principal='rsc', db='rscdb', sql=hr_party_sql)
    hr_party_pdf = pd.DataFrame(res, columns=['gh','xh','dpdm','cjny','tcny','tcyy','qr','dpmc'])
    module_logger.debug(hr_party_pdf)

    dm_dpmc_sql = '''
    select * from standcode.DM_DPMC
    '''
    res = oraclQuery(principal='gxk', sql=dm_dpmc_sql)
    dm_dpmc_pdf = pd.DataFrame(res, columns=['C_ID', 'C_NAME','IS_EXTEND','C_SHORT_NAME',
                                           'C_LEVEL','PARENT_C_ID','PARENT1_C_ID','PARENT2_C_ID',
                                           'PARENT3_C_ID','PARENT4_C_ID'])

    '''
    下面这部分逻辑取代 l 表部分: 因为无法在pandassql中使用group_concat,collect_list等udaf函数, 所以采用df api
    select 
        substring_index(group_concat(a.partyname order by b.C_ID asc ),',',1) dpmc,  --多行转一行
        substring_index(group_concat(a.joinym order by b.C_ID asc),',',1) joinym,
        a.userid 
    from sams.hr_party a
    inner join sams_factdata.dm_dpmc b on a.partyname=b.C_NAME
    group 
        by a.userid


    checked
    '''
    l_raw_sql = '''
    select 
        a.dpmc,
        a.cjny joinym,
        b.C_ID,
        trim(a.gh) userid -- 必须trim, 坑
    from hr_party_pdf a
    inner join dm_dpmc_pdf b 
    on a.dpmc=b.C_NAME
    '''
    l_raw_pdf = sqldf(l_raw_sql)
    l_raw_pdf = l_raw_pdf.groupby('userid', group_keys=False).apply(lambda x: x.sort_values('C_ID', ascending=True))
    l_raw_pdf.fillna('', inplace=True)
    l_dpmc_pdf = l_raw_pdf.groupby('userid', as_index=False).apply(lambda x: ','.join(x.dpmc))
    l_joinym_pdf = l_raw_pdf.groupby('userid', as_index=False).apply(lambda x: ','.join(x.joinym))
    l_dpmc_pdf.columns=['userid','tmp']
    l_joinym_pdf.columns=['userid','tmp']
    l_dpmc_pdf['dpmc'] = l_dpmc_pdf['tmp'].str.split(',').str.get(0)
    l_joinym_pdf['joinym'] = l_joinym_pdf['tmp'].str.split(',').str.get(0)
    del(l_dpmc_pdf['tmp'])
    del(l_joinym_pdf['tmp'])
    l_pdf = pd.merge(l_dpmc_pdf, l_joinym_pdf, on='userid')
    del l_raw_pdf
    del l_dpmc_pdf
    del l_joinym_pdf


    '''
    replace: sams.hr_academicdeg
    checked: select * from sams.hr_academicdeg where scbj = 'N'
             [42206 rows x 3 columns]
    '''
    hr_academicdeg_sql = '''
    SELECT   
        A.gh AS userid, 
        A.byxx AS schoolname, 
        A.xlm AS educationdegcod,
        A.xwm AS academiccode
    FROM
        dbo.XWXLK AS A LEFT OUTER JOIN
        dbo.DM_ZY AS B ON A.sxzym = B.dm LEFT OUTER JOIN
        dbo.DM_GB4658 AS C ON A.xlm = C.dm LEFT OUTER JOIN
        dbo.DM_GB6864 AS D ON A.xwm = D.dm
    WHERE  (A.xh > 0) AND (A.qr = 2) AND (A.scbj='N' or A.scbj is null)
    '''
    res = sqlServerQuery(principal='rsc', db='rscdb', sql=hr_academicdeg_sql)
    hr_academicdeg_pdf = pd.DataFrame(res, columns=['userid','schoolname','educationdegcode', 'academiccode'])


    '''
    下面这部分逻辑取代 ccc 表部分: 因为无法在pandassql中使用group_concat,collect_list等udaf函数, 所以采用 df api
    select 
        userid,
		substring_index(group_concat(ifnull(schoolname,'') order by educationdegcode),',',1)as schoolname,
        substring_index(group_concat(educationdegcode order by educationdegcode),',',1) as educationdegcode
    from sams.hr_academicdeg 
    where educationdegcode>'00' 
    group by 
        userid

    checked: [21739 rows x 2 columns]
    '''
    ccc_raw_sql = '''
    select
        trim(userid) userid, -- 必须trim, 坑
        schoolname,
        educationdegcode
    from hr_academicdeg_pdf  
    where educationdegcode>'00' 
    '''
    ccc_raw_pdf = sqldf(ccc_raw_sql)
    ccc_raw_pdf = ccc_raw_pdf.groupby('userid', group_keys=False).apply(lambda x: x.sort_values('educationdegcode', ascending=True))
    ccc_raw_pdf.fillna('', inplace=True)
    ccc_schoolname_pdf = ccc_raw_pdf.groupby('userid', as_index=False).apply(lambda x: ','.join(x.schoolname))
    ccc_educationdegcode_pdf = ccc_raw_pdf.groupby('userid', as_index=False).apply(lambda x: ','.join(x.educationdegcode))
    ccc_schoolname_pdf.columns=['userid','tmp']
    ccc_educationdegcode_pdf.columns=['userid','tmp']
    ccc_schoolname_pdf['schoolname'] = ccc_schoolname_pdf['tmp'].str.split(',').str.get(0)
    ccc_educationdegcode_pdf['educationdegcode'] = ccc_educationdegcode_pdf['tmp'].str.split(',').str.get(0)
    del(ccc_schoolname_pdf['tmp'])
    del(ccc_educationdegcode_pdf['tmp'])
    ccc_pdf = pd.merge(ccc_schoolname_pdf, ccc_educationdegcode_pdf, on='userid')
    del ccc_raw_pdf
    del ccc_schoolname_pdf
    del ccc_educationdegcode_pdf


    '''
    下面这部分逻辑取代 ccc_2 表部分: 因为无法在pandassql中使用group_concat,collect_list等udaf函数, 所以采用 df api
    select 
        userid,
        academiccode,
		substring_index(group_concat(ifnull(schoolname,'') order by substring(academiccode,1,1)),',',1)as schoolname,
        substring_index(group_concat(educationdegcode order by substring(academiccode,1,1)),',',1) as educationdegcode
    from sams.hr_academicdeg where substring(academiccode,1,1)>'0' group by userid

    checked: [15864 rows x 2 columns]
    '''
    ccc_2_raw_sql = '''
    select
        trim(userid) userid, -- 必须trim, 坑
        schoolname,
        educationdegcode,
        academiccode
    from hr_academicdeg_pdf  
    where substr(academiccode,1,1) > '0'
    '''
    ccc_2_raw_pdf = sqldf(ccc_2_raw_sql)
    ccc_2_raw_pdf = ccc_2_raw_pdf.groupby('userid', group_keys=False).apply(lambda x: x.sort_values('educationdegcode', ascending=True))
    ccc_2_raw_pdf.fillna('', inplace=True)
    ccc_2_schoolname_pdf = ccc_2_raw_pdf.groupby('userid', as_index=False).apply(lambda x: ','.join(x.schoolname))
    ccc_2_educationdegcode_pdf = ccc_2_raw_pdf.groupby('userid', as_index=False).apply(lambda x: ','.join(x.educationdegcode))
    ccc_2_schoolname_pdf.columns=['userid','tmp']
    ccc_2_educationdegcode_pdf.columns=['userid','tmp']
    ccc_2_schoolname_pdf['schoolname'] = ccc_2_schoolname_pdf['tmp'].str.split(',').str.get(0)
    ccc_2_educationdegcode_pdf['educationdegcode'] = ccc_2_educationdegcode_pdf['tmp'].str.split(',').str.get(0)
    del(ccc_2_schoolname_pdf['tmp'])
    del(ccc_2_educationdegcode_pdf['tmp'])
    ccc_2_pdf = pd.merge(ccc_2_schoolname_pdf, ccc_2_educationdegcode_pdf, on='userid')
    del ccc_2_raw_pdf
    del ccc_2_schoolname_pdf
    del ccc_2_educationdegcode_pdf


    '''
    replace: sams_factdata.DM_GB4658
    checked: select * from sams_factdata.DM_GB4658
    '''
    DM_GB4658_sql = '''
    select * from DM_GB4658
    '''
    res = sqlServerQuery(principal='rsc', db='rscdb', sql=DM_GB4658_sql)
    DM_GB4658_pdf = pd.DataFrame(res, columns=['dm','mc','isGB', 'isCheck', 'hasParent'])    
    
    '''
    replace: sams_factdata.DM_GB4658
    checked: select * from sams_factdata.DM_GB4658
    '''
    DM_GB6864_sql = '''
    select * from DM_GB6864 where isCheck = 'Y'
    '''
    res = sqlServerQuery(principal='rsc', db='rscdb', sql=DM_GB6864_sql)
    DM_GB6864_pdf = pd.DataFrame(res, columns=['dm','mc','isGB', 'isCheck', 'hasParent'])

    hr_basic_S_add_attrs = f'''
    select
        a.userid,
        a.NAME_CERT name,
        a.gender,
        a.birthdate,
        a.idcardcode,
        a.nation,
        a.nationcode,
        case when l.dpmc='中国共产党' then '中共党员' 
             when l.dpmc='中国共产党预备党员' then '中共预备党员' 
             else l.dpmc end partisan_assemble,
        a.TELEPHONE phone,
        a.jaccount,
        g.mc xlmc,
        h.mc xwmc,
        ccc.schoolname graduate_school,
        ccc_2.schoolname graduate_school_academic,
        a.facultycode,
        a.faculty,
        a.superiorfacultycode,
        a.superiorfaculty,
        a.jzgmblc,
        a.gwmc,
        a.bzmc,
        a.print_mc,
        a.ryzt
    from hr_basic_S_pdf a
    left join l_pdf l on trim(a.userid) = trim(l.userid)
    left join ccc_pdf ccc on trim(a.userid) = trim(ccc.userid)
    left join ccc_2_pdf ccc_2 on trim(a.userid) = trim(ccc_2.userid)
    left join DM_GB4658_pdf g on trim(ccc.educationdegcode) = trim(g.dm)
    left join DM_GB6864_pdf h on trim(ccc_2.educationdegcode) = trim(h.dm)
    where a.userid in ({target_ry_str}) 
    or superiorfacultycode in ({target_dw_str})
    '''
    module_logger.debug(hr_basic_S_add_attrs)
    hr_basic_S_add_attrs_pdf = sqldf(hr_basic_S_add_attrs)

    # TODO sink inc to ZJ
    if hr_basic_S_add_attrs_pdf.shape[0] > 0:
        module_logger.info(f'准备写入张江高研院【{execution_date_15m_ago},{execution_date}）增量数据 ---> JG_JBXX_CAMPUS')
        upsertDfTo_JG_JBXX_CAMPUS(zj_principal, hr_basic_S_add_attrs_pdf, target_tbl)
        del hr_basic_S_add_attrs_pdf
    

    # At the sametime, Query Incremental data from GXK
    # If has_new = True. Push all data to ZJ
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
        sql = f"""select 
            sjc 
        from JG_JBXX
        where sjc >= '{execution_date_15m_ago}'
        and sjc < '{execution_date}'"""
        res = oraclQueryWithDDL(principal='gxk', preDDL="alter session set nls_timestamp_format = 'YYYY-MM-DD HH24:MI:SS.FF'", sql=sql)
        module_logger.info(f'hr_basic 更新了 {len(res)} 条数据')
        module_logger.info(res)

        if res:
            # 如果共享库JG_JBXX有更新，则全量推一遍数据
            all_sql = f'''
            select
                a.userid,
                a.NAME_CERT name,
                a.gender,
                a.birthdate,
                a.idcardcode,
                a.nation,
                a.nationcode,
                case when l.dpmc='中国共产党' then '中共党员' 
                    when l.dpmc='中国共产党预备党员' then '中共预备党员' 
                    else l.dpmc end partisan_assemble,
                a.TELEPHONE phone,
                a.jaccount,
                g.mc xlmc,
                h.mc xwmc,
                ccc.schoolname graduate_school,
                ccc_2.schoolname graduate_school_academic,
                a.facultycode,
                a.faculty,
                a.superiorfacultycode,
                a.superiorfaculty,
                a.jzgmblc,
                a.gwmc,
                a.bzmc,
                a.print_mc,
                a.ryzt
            from hr_basic_S_pdf a
            left join l_pdf l on trim(a.userid) = trim(l.userid)
            left join ccc_pdf ccc on trim(a.userid) = trim(ccc.userid)
            left join ccc_2_pdf ccc_2 on trim(a.userid) = trim(ccc_2.userid)
            left join DM_GB4658_pdf g on trim(ccc.educationdegcode) = trim(g.dm)
            left join DM_GB6864_pdf h on trim(ccc_2.educationdegcode) = trim(h.dm)
            where a.userid in ({synced_ry_str}) 
            or superiorfacultycode in ({synced_dw_str})
            '''
            all_df = sqldf(all_sql)
            module_logger.debug('all_df:')
            module_logger.debug(f'\n{all_df}')

            if all_df.shape[0] > 0:
                module_logger.info('准备写入张江高研院当前全量数据 ---> JG_JBXX_CAMPUS')
                upsertDfTo_JG_JBXX_CAMPUS(zj_principal, all_df, target_tbl)
    



























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