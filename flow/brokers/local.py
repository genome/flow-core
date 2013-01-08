import logging
from collections import deque, defaultdict

LOG = logging.getLogger(__name__)


class LocalBroker(object):
    def __init__(self):
        self.queue = deque()
        self.bindings = defaultdict(list)
        self.handlers = {}

    def register_handler(self, queue_name, handler):
        self.handlers[queue_name] = handler

    def register_binding(self, routing_key, queue_name):
        self.bindings[routing_key].append(queue_name)

    def publish(self, routing_key, message):
        self.queue.append((routing_key, message))

    def listen(self):
        while self.queue:
            routing_key, message = self.queue.popleft()
            LOG.debug('got message on rk %s: %s', routing_key, message)
            queues = self.bindings[routing_key]
            for q in queues:
                try:
                    h = self.handlers[q]
                    # XXX I like passing the broker into handlers...
#                    h(message, self)
                    h(message)
                except KeyError:
                    pass
        else:
            LOG.warning('No messages found in queue.')
