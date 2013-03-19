from abc import ABCMeta, abstractmethod
import logging

LOG = logging.getLogger(__name__)

class BrokerBase(object):
    __metaclass__ = ABCMeta

    def publish(self, exchange_name, routing_key, message):
        encoded_message = message.encode()
        self.raw_publish(exchange_name, routing_key, encoded_message)

    @abstractmethod
    def raw_publish(self, exchange_name, routing_key, encoded_message):
        pass

    @abstractmethod
    def register_handler(self, handler):
        pass
