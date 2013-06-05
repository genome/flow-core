DEFAULT_FLOW_CONFIG_PATH = '/etc/flow'

DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'root': {
        'level': 'INFO',
        'handlers': ['console'],
    },

    'formatters': {
        'plain': {
            'format': '%(asctime)s %(levelname)s %(name)s %(funcName)s '
                        '%(lineno)d: %(message)s',
        },
    },

    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'plain',
        },
    },

    'loggers': {
        'flow': {
            'level': 'INFO',
        },
        'pika': {
            'level': 'INFO',
        },
    },
}
