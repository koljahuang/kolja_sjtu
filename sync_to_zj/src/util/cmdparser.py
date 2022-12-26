import argparse

def get_args():
    parser = argparse.ArgumentParser(description='Sync data to ZJ.')
    parser.add_argument('-m', '--mode', choices=['dev', 'prod'], dest='mode', help='mode')
    parser.add_argument('-ed', '--execution_date', dest='execution_date', help='batch execution date')
    parser.add_argument('-init', '--is_init', dest='is_init', help='is init?')
    # TODO 从哪里同步到哪里
    args = parser.parse_args()
    return args