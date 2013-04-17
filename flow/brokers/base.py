from abc import ABCMeta, abstractmethod
from flow.util import stats

import flow.interfaces
import logging


LOG = logging.getLogger(__name__)


class BrokerBase(flow.interfaces.IBroker):
    def publish(self, exchange_name, routing_key, message):
        timer = stats.create_timer('messages.publish.%s' % message.__class__.__name__)
        timer.start()

        encoded_message = message.encode()
        timer.split('encode')

        self.raw_publish(exchange_name, routing_key, encoded_message)
        timer.split('publish')
        timer.stop()
