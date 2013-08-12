import unittest
try:
    from unittest import mock
except:
    import mock

from twisted.python.failure import Failure
from twisted.internet import defer

from flow.handler import Handler
from flow.protocol.exceptions import InvalidMessageException

class TestHandler(Handler):
    message_class = mock.Mock
    message_class.decode = mock.Mock()
    message_class.decode.return_value = True

    def __init__(self):
        self._handle_message = mock.Mock()

    def _handle_message(self, message):
        # this just satisfies the ABC declaration
        pass

class HandlerTest(unittest.TestCase):
    def setUp(self):
        self.handler = TestHandler()
        self.message = self.handler.message_class()

    def tearDown(self):
        # keep twisted from getting the exception
        if hasattr(self, 'deferred'):
            self.deferred.addErrback(lambda *args: None)

    def test_call_succeed(self):
        self.handler._handle_message.return_value = defer.succeed(None)
        self.deferred = self.handler(self.message)

        self.assertTrue(self.deferred.called)
        self.assertEqual(self.deferred.result, None)

    def test_call_fail(self):
        self.handler._handle_message.return_value = defer.fail(RuntimeError)
        self.deferred = self.handler(self.message)

        self.assertTrue(self.deferred.called)
        self.assertTrue(isinstance(self.deferred.result, Failure))
