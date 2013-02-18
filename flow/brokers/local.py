import logging
from collections import deque, defaultdict
from flow.protocol import codec

from flow.brokers.base import BrokerBase

LOG = logging.getLogger(__name__)


class LocalBroker(BrokerBase):
    def __init__(self, bindings):
        self.bindings = _transform_bindings(bindings)
        self.queue = deque()
        self.handlers = {}

    def register_handler(self, handler):
        self.handlers[handler.queue_name] = handler

    def raw_publish(self, exchange, routing_key, encoded_message):
        LOG.debug('Putting message for exchange (%s), '
                  'routing_key (%s) in queue: %s',
                  exchange, routing_key, encoded_message)
        self.queue.append((exchange, routing_key, encoded_message))

    def connect_and_listen(self):
        return self.listen()

    def listen(self):
        while self.queue:
            exchange, routing_key, encoded_message = self.queue.popleft()
            message = codec.decode(encoded_message)
            LOG.debug('got message on exchange %s via routing_key %s: %s',
                    exchange, routing_key, message)
            queues = self.bindings[exchange][routing_key]
            for q in queues:
                try:
                    h = self.handlers[q]
                    h(message)
                except KeyError:
                    pass
        else:
            LOG.warning('No messages found in queue.')

def _transform_bindings(source_bindings):
    result = defaultdict(lambda: defaultdict(list))
    for exchange, queue_rk_map in source_bindings.iteritems():
        routes = result[exchange]
        for queue, rks in queue_rk_map.iteritems():
            for rk in rks:
                routes[rk].append(queue)

    return result
