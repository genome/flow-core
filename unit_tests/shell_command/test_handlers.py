from flow.shell_command.handler import LSFShellCommandMessageHandler
from twisted.internet import defer
from twisted.python.failure import Failure

import flow.shell_command.handler
import mock
import unittest


class ShellCommandMessageHandlerTest(unittest.TestCase):
    def setUp(self):
        self.executor = mock.Mock()

        orchestrator = mock.Mock()
        self.create_token = mock.Mock()
        self.create_token.return_value = defer.succeed(None)
        orchestrator.create_token = self.create_token

        service_locator = {'orchestrator':orchestrator}

        self.exchange = mock.Mock()
        self.routing_key = mock.Mock()

        self.handler = LSFShellCommandMessageHandler(executor=self.executor,
                queue_name='', service_locator=service_locator,
                exchange=self.exchange, response_routing_key=self.routing_key)

        self.net_key = mock.Mock(str)
        self.dispatch_success_place_idx = 1
        self.dispatch_failure_place_idx = 2
        self.response_places = {
                'post_dispatch_success': self.dispatch_success_place_idx,
                'post_dispatch_failure': self.dispatch_failure_place_idx
        }

        self.message = mock.Mock(self.handler.message_class)
        self.message.command_line = mock.Mock()
        self.message.token_color = None
        self.message.net_key = self.net_key
        self.message.response_places = self.response_places
        self.message.executor_options = {'passthru': True}

    def test_message_handler_executor_success(self):
        job_id = 1234
        self.executor.return_value = (job_id, True)

        deferred = self.handler(self.message)

        # XXX not sure this makes sense
#        self.assertEqual(deferred.called, True)
#        self.assertEqual(deferred.result, None)

        self.executor.assert_called_once_with(self.message.command_line,
                net_key=self.net_key, response_places=self.response_places,
                token_color=self.message.token_color, passthru=True)

        self.create_token.assert_called_once_with(net_key=self.net_key,
                place_idx=self.dispatch_success_place_idx,
                token_color=None, data={'pid': str(job_id)})

    def test_message_handler_executor_failure(self):
        job_id = 1234
        self.executor.return_value = (job_id, False)

        self.message.token_color = 3
        deferred = self.handler(self.message)

        # XXX not sure this makes sense
#        self.assertEqual(deferred.called, True)
#        self.assertEqual(deferred.result, None)

        self.executor.assert_called_once_with(self.message.command_line,
                net_key=self.net_key, response_places=self.response_places,
                token_color=self.message.token_color,
                passthru=True)

        self.create_token.assert_called_once_with(net_key=self.net_key,
                place_idx=self.dispatch_failure_place_idx,
                token_color=3, data={'pid': str(job_id)})


    def test_message_handler_executor_exception(self):
        self.executor.side_effect = RuntimeError

        self.assertRaises(RuntimeError, self.handler._handle_message, self.message)

        self.executor.assert_called_once_with(self.message.command_line,
                net_key=self.net_key, response_places=self.response_places,
                token_color=self.message.token_color, passthru=True)

        # handler.__call__ returns failed deferred
        deferred = self.handler(self.message)
        self.assertEqual(deferred.called, True)
        self.assertTrue(isinstance(deferred.result, Failure))

        # XXX This is not ok.  We need to start using twisted.trial
        # keep twisted from catching the exception.
        deferred.addErrback(lambda *args: None)


if '__main__' == __name__:
    unittest.main()
