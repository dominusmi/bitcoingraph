import logging
import os

logging.basicConfig(format='%(levelname)s::%(filename)s:%(asctime)s::%(message)s')
root_logger = logging.getLogger("bitcoingraph")

LOGLEVEL = os.environ.get('BCG_LOGLEVEL', 'INFO').upper()
root_logger.setLevel(LOGLEVEL)
cached_loggers = {}

def get_logger(name):
    global cached_loggers
    logger = cached_loggers.get(name)
    if logger is not None:
        return logger

    logger = root_logger.getChild(name)

    formatter = logging.Formatter('[%(levelname)s] - [%(name)s] - %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.propagate = False

    return logger