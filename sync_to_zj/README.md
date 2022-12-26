# sa view: V_JG_JBXX_CAMPUS V_XS_JBXX_CAMPUS(gao)
    '''
    select 
        `adl`.`userid` AS `userid`,
        `adl`.`name` AS `name`,
        `adl`.`gender` AS `gender`,
        `adl`.`birthdate` AS `birthdate`,
        `basic`.`idcardcode` AS `idcardcode`,
        `basic`.`nation` AS `nation`,
        `basic`.`nationcode` AS `nationcode`,
        `adl`.`partisan_assemble` AS `partisan_assemble`,  #111
        ifnull(`adl`.`phone_defined`,`adl`.`phone`) AS `phone`,
        `basic`.`jaccount` AS `jaccount`,
        `adl`.`xlmc` AS `xlmc`,  #111
        `adl`.`xwmc` AS `xwmc`,  #111
        `adl`.`graduate_school` AS `graduate_school`,  #111
        `adl`.`graduate_school_academic` AS `graduate_school_academic`, #111
        `basic`.`faculty` AS `faculty`,
        `basic`.`facultycode` AS `facultycode`,
        `basic`.`superiorfacultycode` AS `superiorfacultycode`,
        `basic`.`superiorfaculty` AS `superiorfaculty`,

        `adl`.`jzgmblc` AS `jzgmblc`,
        `adl`.`gwmc` AS `gwmc`,
        `adl`.`bzmc` AS `bzmc`,
        `adl`.`print_mc` AS `print_mc`,
        case when `adl`.`ryztm` = '9' then '在职' when '99' then '返聘' when '4' then '离职' when '2' then '离退休' else '其它' end AS `ryztm` 
        from 
            (
                `sams_data`.`teacher_info_all_adl` `adl` 
                left join 
                    `sams`.`hr_basic` `basic` 
                on (`adl`.`userid` = `basic`.`userid` and `basic`.`usertype` = 'S')
            ) 
        where `basic`.`superiorfacultycode` in ('42600','32600','63200','32500') or `basic`.`userid` in (...)
    '''
    
    # extends sams.hr_basic, sams_data.teacher_info_all_adl
        # sams.hr_basic from 共享库shareddb服务中：`jg_jbxx`, 一堆维度视图，`identity_auth_relation`， `identity_auth_account`
                                 standcode服务中：一堆维表
        # sams_data.teacher_info_all_adl from sams.hr_basic, sams_factdata.jg_jbxx,....


    '''
    CREATE ALGORITHM=UNDEFINED DEFINER=`sams`@`sams-%` SQL SECURITY DEFINER VIEW `V_XS_JBXX_CAMPUS` AS 
    select 
        `jbk`.`XH` AS `XH`,`jbk`.`XM` AS `XM`,`jbk`.`XMPY` AS `XMPY`,`jbk`.`XBDM` AS `XBDM`,`xb`.`C_NAME` AS `XBMC`,`jbk`.`CSRQ` AS `CSRQ`,`jbk`.`MZDM` AS `MZDM`,`mz`.`MZMC` AS `MZMC`,`jbk`.`ZZMMDM` AS `ZZMMDM`,`zzmm`.`C_NAME` AS `ZZMMMC`,`jbk`.`JGDM` AS `JGDM`,`jg`.`MC` AS `JGMC`,`hyzk`.`mc` AS `HYZKMC`,`jbk`.`GBDM` AS `GBDM`,`gb`.`C_NAME` AS `GBMC`,rtrim(`jbk`.`SFZHM`) AS `SFZHM`,`jbk`.`XLDM` AS `XLDM`,`xl`.`mc` AS `XLMC`,`jbk`.`SYDQM` AS `SYDQM`,`syqdm`.`MC` AS `SYDQMC`,`jbk`.`JTDZ` AS `JTDZ`,`jbk`.`EMAIL` AS `EMAIL`,`jbk`.`SJ` AS `SJ`,`jbk`.`PYFSDM` AS `PYFSDM`,`pyfs`.`C_NAME` AS `PYFSMC`,`jbk`.`YXSH` AS `YXDM`,`yxdmk`.`yxmc` AS `YXMC`,`jbk`.`BH` AS `BH`,`jbk`.`ZYH` AS `ZYDM`,`zyb`.`C_NAME` AS `ZYMC`,`jbk`.`DSGH` AS `DSGH`,`jbk`.`DSXM` AS `DSXM`,`jbk`.`DSGH2` AS `DSGH2`,`jbk`.`DSXM2` AS `DSXM2`,case when `jbk`.`LXBJ` = 0 and `jbk`.`SJLY` = '60300' or `jbk`.`TJ` = 'Y' and `jbk`.`SJLY` = '60200' then 'N' else 'Y' end AS `LXBJ`,case when `jbk`.`LXBJ` = 0 and `jbk`.`SJLY` = '60300' or `jbk`.`TJ` = 'Y' and `jbk`.`SJLY` = '60200' then '在校' else '不在校' end AS `LXBJMC`,`jbk`.`XQDM` AS `XQDM`,case when `jbk`.`SJLY` = '60300' then '研究生' else '本科生' end AS `SJLY`,`jbk`.`XJZTMX` AS `XJZTMX`,`XJZT`.`C_NAME` AS `XJZTMXMC`,`jbk`.`XZ` AS `XZ`,`jbk`.`XXFSDM` AS `XXFSDM`,`XXFS`.`C_NAME` AS `XXFSMC`,`jbk`.`SFLXS` AS `SFLXS`,`mx`.`YJBYSJ` AS `YJBYSJ`,`mx`.`DBRQ` AS `DBRQ`,`mx`.`BYSJ` AS `BYSJ`,`lq`.`RXNJ` AS `RXNJ`,'' AS `BYXWDM`,'' AS `BYXWMC`,`jbk`.`SCBJ` AS `SCBJ`,str_to_date(`jbk`.`SJC`,'%Y-%m-%d %H:%i:%S') AS `SJC` 
    from 
        (((((((((((((((`xs_xsjbk` `jbk` left join `dm_xb` `xb` on(`jbk`.`XBDM` = `xb`.`C_ID`)) left join `DM_MZ` `mz` on(`jbk`.`MZDM` = `mz`.`MZDM`)) left join `dm_zzmm` `zzmm` on(`jbk`.`ZZMMDM` = `zzmm`.`C_ID`)) left join `DM_GB2260` `jg` on(`jbk`.`JGDM` = `jg`.`DM`)) left join `DM_GB4766` `hyzk` on(`jbk`.`HYZKDM` = `hyzk`.`dm`)) left join `dm_gjdqmc` `gb` on(`jbk`.`GBDM` = `gb`.`C_ID`)) left join `DM_GB4658` `xl` on(`jbk`.`XLDM` = `xl`.`dm`)) left join `DM_GB2260` `syqdm` on(`jbk`.`JGDM` = `syqdm`.`DM`)) left join `DM_PYFS_ALL` `pyfs` on(`jbk`.`PYFSDM` = `pyfs`.`C_ID` and `jbk`.`SJLY` = `pyfs`.`SJLY`)) left join `DM_YXDMK` `yxdmk` on(`jbk`.`YXSH` = `yxdmk`.`yxdm`)) left join `DM_ZYB_ALL` `zyb` on(`jbk`.`ZYH` = `zyb`.`C_ID` and `jbk`.`SJLY` = `zyb`.`SJLY`)) left join `DM_XJZT_ALL` `XJZT` on(`jbk`.`XJZTMX` = `XJZT`.`C_ID` and `jbk`.`SJLY` = `XJZT`.`SJLY`)) left join `DM_XXFS_ALL` `XXFS` on(`jbk`.`XXFSDM` = `XXFS`.`C_ID` and `jbk`.`SJLY` = `XXFS`.`SJLY`)) left join `XS_BY` `mx` on(`jbk`.`XH` = `mx`.`XH`)) left join `xs_kslq` `lq` on(`jbk`.`XH` = `lq`.`XH`)) where `jbk`.`YXSH` in ('42600','32600','63200') or `jbk`.`XH` in ()
    '''

# 综上所述：从业务库直接监听增量数据的开发量较大，监听共享库的比较中庸：既减少耦合度，又减少开发量

# write to jsc.JG_JBXX_CAMPUS
# read from jsc.TB_DW_CAMPUS
# read from jsc.TB_RY_CAMPUS


# 注意1：本套代码适合100w级别以下的数据同步，不适合大数据量的同步
# 注意2: 因为很多维度表没有时间戳，所以一jbxx为基准表，如果jbxx中有增量更新，就全量推一遍数据，否则维度表的变更在张江已同步的数据中无法获得更新