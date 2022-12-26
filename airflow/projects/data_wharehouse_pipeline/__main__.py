'''
parse cmd args which delieverd by airflow
'''

import sys
import traceback
from config import settings
from src.util.cmdparser import get_args
from src.util.deco import log_wrapper, process_duration
from src.core.bdp_mysql_ops import release_flow
from src.flow import run

LOG_NAME = settings.LOG_NAME

@log_wrapper(LOG_NAME)
@process_duration
def main(*args, **kwargs) -> None:

    logger = args[0]

    cmd_args = get_args()

    '''set env'''
    mode = cmd_args.mode
    settings.setenv(mode)
    logger.info(f'current env: {settings.current_env}')

    '''start etl flow'''
    try:
        for k, v in vars(cmd_args).items():
            logger.info(f'{k}: {v}')
        run(vars(cmd_args))
    except Exception as e:
        release_flow(cmd_args.flow_name)
        logger.warning(f'release lock cause {e}')
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
