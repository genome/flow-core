import logging
from collections import deque, defaultdict

LOG = logging.getLogger(__name__)


class LocalBroker(object):
    def __init__(self):
        self.queue = deque()
        self.bindings = defaultdict(list)
        self.handlers = defaultdict(list)

    def register_handler(self, queue_name, handler):
        self.handlers[queue_name].append(handler)

    def register_binding(self, routing_key, queue_name):
        self.bindings[routing_key].append(queue_name)

    def publish(self, routing_key, message):
        self.queue.append((routing_key, message))

    def listen(self):
        while len(self.queue):
            routing_key, message = self.queue.popleft()
            queues = self.bindings[routing_key]
            for q in queues:
                for h in self.handlers[q]:
                    # XXX I like passing the broker into handlers...
#                    h(message, self)
                    h(message)

        LOG.info('No messages remain in queue.')
