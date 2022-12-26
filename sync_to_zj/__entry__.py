'''
parse cmd args
'''
import sys
import traceback
from config import settings
from src.util.cmdparser import get_args
from src.util.deco import log_wrapper, process_duration
from src.sync import run

LOG_NAME = settings.LOG_NAME

@log_wrapper(LOG_NAME)
@process_duration
def main(*args, **kwargs) -> None:

    logger = args[0]

    cmd_args = get_args()
    print(cmd_args)

    '''set env'''
    mode = cmd_args.mode
    settings.setenv(mode)
    logger.info(f'current env: {settings.current_env}')

    '''export env var cmd_args'''
    settings.CMD_ARGS = vars(cmd_args)

    '''start sync'''
    try:
        for k, v in vars(cmd_args).items():
            logger.info(f'{k}: {v}')
        run()
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)
    


if __name__ == '__main__':
    main()
