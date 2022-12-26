import logging
import sys
from config import settings


LOG_LEVEL = settings.LOG_LEVEL


def log(name: str) -> tuple:
    logger = logging.getLogger(name)
    logger.propagate = False
    if LOG_LEVEL.lower() == 'info':
        logger.setLevel(logging.INFO)
    elif LOG_LEVEL.lower() == 'debug':
        logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(process)d - %(asctime)s - [%(filename)s:%(name)s:%(lineno)d] - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    from .DtFormat import DtFormat
    import os   
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.abspath(os.path.dirname(current_dir)+os.path.sep+".")
    grand_parent_dir = os.path.abspath(
        os.path.dirname(parent_dir)+os.path.sep+".")
    file_handler = logging.FileHandler(
        os.path.join(grand_parent_dir, 'logs/{}.log'.format(DtFormat.yyyy_MM_dd_hh_mm_ss.value)),
        mode='w')
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger, console_handler, file_handler
