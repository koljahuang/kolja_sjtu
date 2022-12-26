from sync_sa_bi.start_sync import start_sync
from sync_sa_bi.log import log

if __name__ == '__main__':
    logger, console_handler, file_handler = log('comparative_analysis')
    start_sync()
    logger.removeHandler(console_handler)
    logger.removeHandler(file_handler)