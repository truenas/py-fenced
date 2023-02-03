import logging
import logging.config
import logging.handlers
import os

LOG_FILE = '/var/log/fenced.log'


def ensure_logdir_exists():
    """
    We need to ensure that the directory in `LOG_FILE` exists
    so logging works
    """
    dirname = os.path.dirname(LOG_FILE)
    os.makedirs(dirname, exist_ok=True)


def setup_logging(foreground):
    ensure_logdir_exists()
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '[%(asctime)s (%(levelname)s) %(name)s:%(funcName)s():%(lineno)d] - %(message)s',
                'datefmt': '%m-%d-%Y %H:%M:%S',
            },
        },
        'handlers': {
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'simple',
                'level': 'INFO',
                'filename': LOG_FILE,
                'maxBytes': 1000000,  # 1MB size
                'backupCount': '3',
            },
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'simple',
                'level': 'DEBUG' if foreground else 'INFO',
                'stream': 'ext://sys.stdout',
            },
        },
        'loggers': {
            '': {
                'handlers': ['console', 'file'],
                'level': 'DEBUG',
                'propagate': True,
            },
        },
    })
