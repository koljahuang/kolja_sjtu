import logging,sys,os,datetime

def log(name: str) -> tuple:
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(process)d - %(asctime)s - [%(filename)s:%(name)s:%(lineno)d] - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)


    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.abspath(os.path.dirname(current_dir)+os.path.sep+".")
    file_handler = logging.FileHandler(
        os.path.join(parent_dir, f'logs/sync_{datetime.datetime.now().strftime("%Y-%m-%d")}.log'),
        mode='w')
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger, console_handler, file_handler