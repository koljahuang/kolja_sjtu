import logging
from config import settings
from src.util.deco import conn_cursor_wrapper

LOG_NAME = settings.LOG_NAME
module_logger = logging.getLogger(f'{LOG_NAME}')


def get_hive_tables(hive_db: str) -> list:
    '''
    visit hive meta db to acquire tables list
    '''
    @conn_cursor_wrapper('metastore')
    def __get_hive_tables(conn, cursor, sql) -> list:
        module_logger.info(f'get_hive_tables: {sql}')
        cursor.execute(sql)
        hive_tables = cursor.fetchall()
        return hive_tables
    
    sql = '''
    select DBS.NAME, TBL_NAME 
    from TBLS
    left join DBS
    on TBLS.DB_ID = DBS.DB_ID
    '''

    tables_from_hive =  __get_hive_tables(conn=None, cursor=None, sql=sql)
    #(('DB1', 'TB1'), ('DB2', 'TB2'))
    
    tables_from_hive = list(map(lambda x: (x[0], x[1]), filter(lambda x: x[0] == 'ods', tables_from_hive)))
    #[('ods', 'TB1'), ('ods', 'TB12')]
    return tables_from_hive


def get_hive_tbl_cols(db: str, tbl: str) -> list:
    '''
    When creating a new hive table, 
    the partition field of the table will not be written to the hive metadata table.
    So use hard coding
    '''
    partition_cols = ['dt', 'flowid']

    @conn_cursor_wrapper('metastore')
    def __get_hive_tbl_cols(conn, cursor, sql) -> list:
        module_logger.info(f'get_hive_tbl_cols: {sql}')
        cursor.execute(sql)
        hive_tbl_cols = cursor.fetchall()
        return hive_tbl_cols
    
    sql = f'''
        select 
            COLUMNS_V2.COLUMN_NAME
        from DBS
        left join TBLS
        on DBS.DB_ID = TBLS.DB_ID
        left join COLUMNS_V2
        on TBLS.TBL_ID = COLUMNS_V2.CD_ID
        where DBS.name = "{db}"
        and TBLS.TBL_NAME = "{tbl}"
        order by 
            COLUMNS_V2.INTEGER_IDX
        '''
    hive_tbl_cols = __get_hive_tbl_cols(conn=None, cursor=None, sql=sql)
    hive_tbl_cols = list(map(lambda x: x[0], hive_tbl_cols))
    hive_tbl_cols.extend(partition_cols)
    return hive_tbl_cols


class ExpandedProcessor:
    '''
    Processing logic to be added in the future
    '''

    def __init__(self):
        module_logger.info("expanded process")


    def expanded_op1(self, rdd):
        pass