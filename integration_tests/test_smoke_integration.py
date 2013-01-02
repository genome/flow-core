import unittest
try:
    from unittest import mock
except:
    import mock

from amqp_manager import channel_manager
from amqp_manager import connection_manager
from amqp_manager import exchange_manager
from amqp_manager import queue_manager

import os

if os.getenv('LOG_TESTS'):
    from amqp_service import log_formatter
    import logging
    LOG_LEVEL = logging.DEBUG
    LOG_FORMAT = ('%(levelname)-23s %(asctime)s %(name)-60s %(funcName)-45s'
                  ' %(lineno)5d: %(message)s')
    LOG = logging.getLogger()
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter.ColorFormatter(LOG_FORMAT))
    console_handler.setLevel(LOG_LEVEL)
    LOG.addHandler(console_handler)
    LOG.setLevel(LOG_LEVEL)

class MockConnection(object):
    def __init__(self, channel=None, close_method_frame=None):
        self._channel = channel
        self.close_method_frame = close_method_frame

    def add_on_close_callback(self, callback):
        self._on_close_callback = callback

    def close(self):
        self._on_close_callback(self)

    def channel(self, callback):
        callback(self._channel)

class MockChannel(object):
    def __init__(self, qd_method_frame=None, ed_method_frame=None):
        self._qd_method_frame = qd_method_frame
        self._ed_method_frame = ed_method_frame

    def add_on_close_callback(self, callback):
        self._on_close_callback = callback

    def close(self):
        self._on_close_callback()

    def queue_declare(self, callback, queue_name, **kwargs):
        callback(self._qd_method_frame)

    def exchange_declare(self, callback, name, **kwargs):
        callback(self._ed_method_frame)


class IntegrationTest(unittest.TestCase):
    def setUp(self):
        self.queue_name = mock.Mock()
        self.bad_data_handler = mock.Mock()
        self.message_handler = mock.Mock()
        self.queue_mgr = queue_manager.QueueManager(self.queue_name,
                bad_data_handler=self.bad_data_handler,
                message_handler=self.message_handler)

        self.exchange_name = mock.Mock()
        self.exchange_mgr = exchange_manager.ExchangeManager(self.exchange_name)

        self.chan_mgr = channel_manager.ChannelManager(
                delegates=[self.queue_mgr, self.exchange_mgr])

        self.url = mock.Mock()
        self.conn_mgr = connection_manager.ConnectionManager(self.url,
                delegates=[self.chan_mgr])

        self.qd_method_frame = mock.Mock()
        self.ed_method_frame = mock.Mock()
        self.channel = MockChannel(
                qd_method_frame=self.qd_method_frame,
                ed_method_frame=self.ed_method_frame)
        self.channel.basic_consume = mock.Mock()

        self.connection = MockConnection(channel=self.channel)
        self.connection.ioloop = mock.Mock()

    def test_on_connection_open(self):
        ready_callback = mock.Mock()
        self.conn_mgr.add_ready_callback(ready_callback)
        self.conn_mgr._on_connection_open(self.connection)

        ready_callback.assert_called_once_with(self.conn_mgr)
        self.channel.basic_consume.assert_called_once()

    def test_on_connection_closed(self):
        self.conn_mgr._on_connection_open(self.connection)
        self.connection.close()


if '__main__' == __name__:
    unittest.main()
