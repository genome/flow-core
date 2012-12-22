import logging
import logging.handlers

try:
    nh = logging.handlers.NullHandler()
except AttributeError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

    nh = NullHandler()

logging.getLogger('amqp_manager').addHandler(nh)

# NOTE pika does not do this itself for some reason
logging.getLogger('pika').addHandler(nh)