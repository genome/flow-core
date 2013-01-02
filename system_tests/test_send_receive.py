import unittest

from amqp_manager import channel_manager
from amqp_manager import connection_manager
from amqp_manager import exchange_manager
from amqp_manager import queue_manager

import uuid
import os

import logging

if os.getenv('LOG_TESTS'):
    from amqp_service import log_formatter

    PIKA_LOG_LEVEL = logging.INFO
    LOG_LEVEL = logging.DEBUG
    LOG_FORMAT = ('%(levelname)-23s %(asctime)s %(name)-60s %(funcName)-45s'
                  ' %(lineno)5d: %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter.ColorFormatter(LOG_FORMAT))
    console_handler.setLevel(LOG_LEVEL)

    LOG = logging.getLogger()
    LOG.addHandler(console_handler)

    LOG.setLevel(LOG_LEVEL)
    logging.getLogger('pika').setLevel(PIKA_LOG_LEVEL)
else:
    import amqp_manager

    LOG = logging.getLogger()
    LOG.addHandler(amqp_manager.nh)


class MockProcess(object):
    def __init__(self, em):
        self.em = em

    def message_handler(self, properties, data, ack_callback, reject_callback):
        LOG.info("Got message (properties = %s): '%s'", properties, data)
        ack_callback()
        LOG.info('Going to shut down.')
        self.cm.stop()

    def bad_data_handler(self, ack_callback, reject_callback):
        reject_callback()

    def set_queue_name(self, queue_manager):
        self.queue_name = queue_manager.queue_name

    def start(self, cm):
        self.cm = cm

        self.em.publish(self.queue_name, 'welcome friends')

class SystemTest(unittest.TestCase):
    def setUp(self):
        self.em = exchange_manager.ExchangeManager('', exchange_type='direct')

        self.process = MockProcess(self.em)

        self.amqp_url = os.environ['TESTING_AMQP_URL']
        self.queue_name = 'system_test_%s' % uuid.uuid4().hex

        self.qm = queue_manager.QueueManager(self.queue_name,
                durable=False, auto_delete=True,
                message_handler=self.process.message_handler,
                bad_data_handler=self.process.bad_data_handler)
        self.qm.add_ready_callback(self.process.set_queue_name)

    def test_basic_channel_manager(self):
        self.chan_man = channel_manager.ChannelManager(
                delegates=[self.em, self.qm])

        self.con_man = connection_manager.ConnectionManager(
                self.amqp_url, delegates=[self.chan_man])

        self.con_man.add_ready_callback(self.process.start)

        self.con_man.start()


if '__main__' == __name__:
    if not os.getenv('TESTING_AMQP_URL'):
        raise RuntimeError('Environment variable TESTING_AMQP_URL must be set')
    unittest.main()
