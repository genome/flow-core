import unittest
try:
    from unittest import mock
except:
    import mock

from flow.amqp_manager import connection_manager

class ConnectionManagerStartStopTest(unittest.TestCase):
    def setUp(self):
        self.connection = mock.Mock()
        self.connection.ioloop = mock.Mock()
        self.connection.ioloop.start = mock.Mock()

        self.url = mock.Mock()
        self.connection_manager = connection_manager.ConnectionManager(self.url)

    def test_start(self):
        pika = mock.Mock()

        pika.URLParameters = mock.Mock()
        connection_parameters = mock.Mock()
        pika.URLParameters.return_value = connection_parameters

        pika.SelectConnection = mock.Mock()
        pika.SelectConnection.return_value = self.connection

        with mock.patch.object(connection_manager, 'pika', pika):
            self.connection_manager.start()

        pika.URLParameters.assert_called_once_with(self.url)
        pika.SelectConnection.assert_called_once_with(connection_parameters,
                self.connection_manager._on_connection_open)
        self.connection.ioloop.start.assert_called_once_with()

    def test_stop(self):
        self.connection.close = mock.Mock()
        self.connection_manager._connection = self.connection

        self.connection_manager.stop()

        self.connection.close.assert_called_once_with()
        self.connection.ioloop.start.assert_called_once_with()

class ConnectionManagerDelegationTest(unittest.TestCase):
    def setUp(self):
        self.url = mock.Mock()
        self.delegates = [mock.Mock(), mock.Mock()]
        self.reconnect_sleep = mock.Mock()

        for delegate in self.delegates:
            delegate.on_connection_open = mock.Mock()
            delegate.on_connection_closed = mock.Mock()

        self.connection_manager = connection_manager.ConnectionManager(
                self.url, delegates=self.delegates,
                reconnect_sleep=self.reconnect_sleep)

    def test_on_connection_open(self):
        connection = mock.Mock()
        connection.add_on_close_callback = mock.Mock()

        self.connection_manager._on_connection_open(connection)
        connection.add_on_close_callback.assert_called_once_with(
                self.connection_manager._on_connection_closed)

        for delegate in self.delegates:
            delegate.on_connection_open.assert_called_once_with(
                    self.connection_manager)

    def test_on_connection_closed(self):
        method_frame = mock.Mock()

        connection = mock.Mock()
        connection.ioloop = mock.Mock()
        connection.ioloop.add_timeout = mock.Mock()
        self.connection_manager._connection = connection

        self.connection_manager._on_connection_closed(method_frame)

        for delegate in self.delegates:
            delegate.on_connection_closed.assert_called_once_with(method_frame)

        connection.ioloop.add_timeout.assert_called_once_with(
                self.reconnect_sleep, self.connection_manager.start)


if '__main__' == __name__:
    unittest.main()
