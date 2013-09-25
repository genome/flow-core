import logging

try:
    logging.getLogger('flow').addHandler(logging.NullHandler())
except AttributeError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

    logging.getLogger('flow').addHandler(NullHandler())
