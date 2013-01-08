import logging
from collections import deque

LOG = logging.getLogger(__name__)

class SingletonMeta(type):
    def __init__(cls, name, bases, dict):
        super(SingletonMeta, cls).__init__(name, bases, dict)
        cls._instance = None

    def __call__(cls,*args,**kw):
        if cls._instance is None:
            cls._instance = super(SingletonMeta, cls).__call__(*args, **kw)
        return cls._instance


_QUEUE = deque()

class Broker(object):
    __metaclass__ = SingletonMeta

    def publish(self, routing_key, message):
        _QUEUE.append((routing_key, message))
        listener = Listener()
        listener.listen()

class Listener(object):
    __metaclass__ = SingletonMeta

    def __init__(self, routing_dictionary):
        self.routing_dictionary = routing_dictionary

    def listen(self):
        routing_key, message = _QUEUE.popleft()

        for handler in self.routing_dictionary[routing_key]:
            try:
                handler(message)
            except:
                LOG.exception('Handler (%s) raised exception on message: %s',
                        handler, message)
        else:
            LOG.warning('No handlers found for routing key (%s) message: %s',
                    routing_key, message)
