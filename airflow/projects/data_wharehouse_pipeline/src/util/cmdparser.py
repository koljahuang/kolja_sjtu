import argparse

def get_args():
    parser = argparse.ArgumentParser(description='Process ETL.')
    parser.add_argument('-m', '--mode', choices=['dev', 'prod'], dest='mode', help='mode')
    parser.add_argument('-fn', '--flow-name', dest='flow_name', help='flow name')
    parser.add_argument('-fid', '--flow-id', dest='flow_id', help='flow id')
    parser.add_argument('-db', '--database', dest='database', help='etl meta database')
    parser.add_argument('-ip', '--input-parall', dest='input_parall', help='input process parallelism', type=int, default=10)
    parser.add_argument('-op', '--output-parall', dest='output_parall', help='output parallelism', type=int, default=-1)
    parser.add_argument('-bp', '--bin-path', dest='binlog_path', help='binlog root path')
    parser.add_argument('-ca', '--category', dest='category', help='binlog category (db, topic) dir')
    parser.add_argument('-hp', '--hive-path', dest='hive_path', help='hive root path', type=str)
    parser.add_argument('-hd', '--hive-db', dest='hive_db', help='hive db, ods/dw', type=str)
    parser.add_argument('-mf', '--max-new-file', dest='max_new_files', help='', type=int, default=500)
    parser.add_argument('-mh', '--max-hour', dest='max_hours', help='', type=int, default=24)

    parser.add_argument('-ft', '--filt-tbs', dest='filter_tbs', help='valid tables to process', default="")
    parser.add_argument('-fd', '--filt-dbs', dest='filter_dbs', help='valid databases to process', default="")

    # parser.add_argument('-ht', '--host', dest='host', help='meta db host', default="")
    # parser.add_argument('-ur', '--user', dest='user', help='meta db user', default="")
    # parser.add_argument('-pd', '--passwd', dest='passwd', help='meta db passwd', default="")
    parser.add_argument('-ll', '--loglevel', dest='log_level', help='spark log level', default="INFO")
    # parser.add_argument('-pt', '--product', dest='product', help='product', default="")
    parser.add_argument('-pf', '--process-fuction', dest='process_fuction', help='', default="")
    # TODO some args move to airflow
    # parser.add_argument('-me', '--max-excutors', dest='max_excutors', help='spark.dynamicAllocation.maxExecutors', default="10")
    args = parser.parse_args()
    return args