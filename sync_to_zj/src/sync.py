import logging
from config import settings
from src.business.gyy_teachers import sync_gyy_teachers
from src.business.gyy_students import sync_gyy_students
# from src.business.zj_card_account import sync_gyy_card


LOG_NAME = settings.LOG_NAME
module_logger = logging.getLogger(f'{LOG_NAME}')

def run() -> None:
    try:
        sync_gyy_teachers('zjgyy')
        # sync_gyy_students('zjgyy')
        # sync_gyy_card('zjgyy')
    except Exception as e:
        # TODO email alert
        module_logger.error(e)


    