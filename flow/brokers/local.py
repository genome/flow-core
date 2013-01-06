import logging

LOG = logging.getLogger(__name__)

class LocalBroker(object):
    def __init__(self, routing_dictionary):
        self.routing_dictionary = routing_dictionary

    def publish(self, routing_key, message):
        for handler in self.routing_dictionary[routing_key]:
            try:
                handler(message)
            except:
                LOG.exception('Handler (%s) raised exception on message: %s',
                        handler, message)
        else:
            LOG.warning('No handlers found for routing key (%s) message: %s',
                    routing_key, message)
