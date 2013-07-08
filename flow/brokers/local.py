from collections import deque, defaultdict
from twisted.internet import defer

import flow.interfaces
import logging


LOG = logging.getLogger(__name__)


class LocalBroker(flow.interfaces.IBroker):
    def __init__(self, bindings):
        self.bindings = _transform_bindings(bindings)
        self.queue = deque()
        self.handlers = {}

    def publish(self, exchange_name, routing_key, message):
        encoded_message = message.encode()
        return self.raw_publish(exchange_name, routing_key, encoded_message)

    def register_handler(self, handler):
        LOG.debug('Registering handler on %s', handler.queue_name)
        self.handlers[handler.queue_name] = handler

    def raw_publish(self, exchange, routing_key, encoded_message):
        LOG.debug('Putting message for exchange (%s), '
                  'routing_key (%s) in queue: %s',
                  exchange, routing_key, encoded_message)
        self.queue.append((exchange, routing_key, encoded_message))
        return defer.succeed(None)

    def connect_and_listen(self):
        return self.listen()

    def listen(self):
        while self.queue:
            exchange, routing_key, encoded_message = self.queue.popleft()
            LOG.debug('got message on exchange %s via routing_key %s: %s',
                    exchange, routing_key, encoded_message)
            queues = self.bindings[exchange][routing_key]
            for q in queues:
                try:
                    h = self.handlers[q]
                    message_class = h.message_class
                    message = message_class.decode(encoded_message)
                    h(message)
                except:
                    LOG.exception('Failed to execute handler')
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
