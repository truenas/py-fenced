import logging
import logging.config
import logging.handlers
import os

LOG_FILE = '/root/syslog/fenced.log'


def ensure_logdir_exists():
    """
    We need to ensure that the directory in `LOG_FILE` exists
    so logging works
    """
    dirname = os.path.dirname(LOG_FILE)
    os.makedirs(dirname, exist_ok=True)


def setup_logging(foreground):
    # ignore annoying ws4py close debug messages
    logging.getLogger('ws4py').setLevel(logging.WARN)

    ensure_logdir_exists()

    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '[%(asctime)s - %(name)s:%(lineno)s] %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
        },
        'handlers': {
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'simple',
                'level': 'ERROR',
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
