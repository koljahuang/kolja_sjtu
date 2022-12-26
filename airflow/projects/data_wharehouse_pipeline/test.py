import MySQLdb,argparse
from src.common.connections.source import koljaSpark

HOST='localhost'
USER='root'
PWD='131438Hugo'
BDP_MYSQL_DB='sjtu_bdp'

def mysql_conn():
    conn = MySQLdb.connect(host='localhost',passwd='131438Hugo',user='root')
    cursor = conn.cursor()
    cursor.execute('select locked from sjtu_bdp.flow_lock where flow_name="xxy"')
    (res) = cursor.fetchone()
    print(res)


def conn_cursor_wrapper(db: str) -> callable:
    host = HOST
    user = USER
    pwd = PWD
    def conn_cursor_inner(run: callable) -> callable: 
        def inner(*args, **kwargs):
            conn = kwargs['conn']
            cursor = kwargs['cursor']
            flow_name = kwargs['flow_name']
            if not conn:
                with MySQLdb.connect(host=host, user=user, passwd=pwd, db=db, connect_timeout=5) as conn:
                        with conn.cursor() as cursor:
                            run(conn, cursor, flow_name)
            else:
                run(conn, cursor, flow_name)
        return inner
    return conn_cursor_inner

@conn_cursor_wrapper(BDP_MYSQL_DB)
def get_flow_lock(conn, cursor, flow_name) -> 'None or tuple':
    '''
    locked=0 means this flow is not locked
    locked=1 means this flow is locked
    '''
    sql = f"show tables;"
    print(sql)
    cursor.execute(sql)
    lock_status = cursor.fetchone()  # tuple or None
    print(lock_status)
    return lock_status

def is_first_flow_processed(flow_name: str) -> bool:
    '''status=None means this flow is first operated'''
    @conn_cursor_wrapper(BDP_MYSQL_DB)
    def __is_first_flow_processed(conn, cursor, flow_name:str) -> bool:
        lock_status = get_flow_lock(conn=conn, cursor=cursor, flow_name=flow_name)
        if lock_status:
            return False
        else:
            return True
    
    return __is_first_flow_processed(conn=None, cursor=None, flow_name=flow_name)


def wrapper(func):
    print(func)
    def inner(*args, **kwargs):
        print(args)
        return func(*args, **kwargs)
    return inner

# @wrapper    
# def f1():
#     print('no arg')

# @wrapper   
# def f2(a):
#     print('1 arg')   

# @wrapper
# def f3(a, b):
#     print('2 arg')

def get_args():
    parser = argparse.ArgumentParser(description='Process ETL.')
    parser.add_argument('mode', choices=['dev', 'prod'], help='mode')
    parser.add_argument('-fn', '--flow-name', dest='flow_name', help='flow name')
    parser.add_argument('-fid', '--flow-id', dest='flow_id', help='flow id')
    parser.add_argument('-db', '--database', dest='database', help='etl meta database')
    parser.add_argument('-ip', '--input-parall', dest='input_parall', help='input process parallelism', type=int, default=50)
    parser.add_argument('-op', '--output-parall', dest='output_parall', help='output parallelism', type=int, default=-1)
    parser.add_argument('-bp', '--bin-path', dest='binlog_path', help='binlog root path')
    parser.add_argument('-ca', '--category', dest='category', help='binlog category (db, topic) dir')
    parser.add_argument('-hp', '--hive-path', dest='hive_path', help='hive root path', type=str)
    parser.add_argument('-hd', '--hive-db', dest='hive_db', help='hive path', type=str)
    parser.add_argument('-mf', '--max-new-file', dest='max_new_files', help='', type=int, default=500)
    parser.add_argument('-mh', '--max-hour', dest='max_hours', help='', type=int, default=24)

    parser.add_argument('-ft', '--filt-table', dest='filter_tables', help='valid table to process', default="")
    parser.add_argument('-fd', '--filt-db', dest='filter_db', help='valid db to process', default="")

    # parser.add_argument('-ht', '--host', dest='host', help='meta db host', default="")
    # parser.add_argument('-ur', '--user', dest='user', help='meta db user', default="")
    # parser.add_argument('-pd', '--passwd', dest='passwd', help='meta db passwd', default="")
    parser.add_argument('-ll', '--loglevel', dest='log_level', help='spark log level', default="INFO")
    parser.add_argument('-pt', '--product', dest='product', help='product', default="")
    parser.add_argument('-pf', '--process-fuction', dest='process_fuction', help='', default="")
    parser.add_argument('-me', '--max-excutors', dest='max_excutors', help='spark.dynamicAllocation.maxExecutors', default="10")
    args = parser.parse_args()
    # args.hive_path = args.hive_path + "/" + args.hive_db
    return args



if __name__ == '__main__':
    # mysql_conn()
    # f3(a=1,b=2)
    is_first_flow_processed('abc')
    # parser = argparse.ArgumentParser(description='Process ETL.')
    # parser.add_argument('-fn', help='flow name')
    # parser.add_argument('-fid', '--flow-id', dest='flow_id', help='flow id')
    # parser.add_argument('-db', '--database', dest='database', help='etl meta database')
    # args = parser.parse_args()
    # args = vars(parser.parse_args())
    # print(args)





