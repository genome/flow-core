import unittest
try:
    from unittest import mock
except:
    import mock

from amqp_service.service import AMQPService


class AMQPServiceTest(unittest.TestCase):
    def setUp(self):
        self.responders = [mock.Mock(), mock.Mock()]
        self.connection_manager = mock.Mock()

        self.service = AMQPService(self.connection_manager, *self.responders)

    def tearDown(self):
        del self.responders
        del self.connection_manager


    def test_run(self):
        self.connection_manager.run = mock.Mock()
        self.connection_manager.register_responder = mock.Mock()
        self.service.run()

        for responder in self.responders:
            self.connection_manager.register_responder.assert_any_call(responder)
        self.connection_manager.run.assert_called_once_with()

    def test_stop(self):
        self.connection_manager.stop = mock.Mock()
        self.service.stop()
        self.connection_manager.stop.assert_called_once_with()


if '__main__' == __name__:
    unittest.main()
