import unittest
try:
    from unittest import mock
except:
    import mock

from flow.petri import SetTokenMessage

import flow.command_runner.handler
from flow.command_runner.handler import CommandLineSubmitMessageHandler

class CommandLineSubmitMessageHandlerTest(unittest.TestCase):
    def setUp(self):
        self.executor = mock.Mock()

        self.broker = mock.Mock()
        self.broker.publish = mock.Mock()
        self.exchange = mock.Mock()
        self.routing_key = mock.Mock()

        self.storage = mock.Mock()

        self.handler = CommandLineSubmitMessageHandler(executor=self.executor,
                broker=self.broker, storage=self.storage,
                exchange=self.exchange, routing_key=self.routing_key)

        self.net_key = mock.Mock(str)
        self.pre_dispatch_place_idx = 0
        self.dispatch_success_place_idx = 1
        self.dispatch_failure_place_idx = 2
        self.response_places = {
                'pre_dispatch': self.pre_dispatch_place_idx,
                'post_dispatch_success': self.dispatch_success_place_idx,
                'post_dispatch_failure': self.dispatch_failure_place_idx
        }

        self.message = mock.Mock()
        self.message.command_line = mock.Mock()
        self.message.net_key = self.net_key
        self.message.response_places = self.response_places
        self.message.executor_options = {'passthru': True}


    def test_set_token(self):
        with mock.patch("flow.command_runner.handler.Token") as T:
            T.create = mock.Mock()
            token = mock.Mock
            token.key = mock.Mock(str)
            T.create.return_value = token

            place_idx = self.dispatch_success_place_idx

            self.handler.set_token(self.net_key, place_idx)

            T.create.assert_called_once_with(self.storage, data=None)

            response_message = SetTokenMessage(token_key=token.key,
                    net_key=self.net_key,
                    place_idx=place_idx)
            self.broker.publish.assert_called_once_with(
                    self.exchange, self.routing_key, response_message)


    def test_message_handler_executor_success(self):
        job_id = 1234
        self.executor.return_value = (job_id, True)

        set_token = mock.Mock()
        self.handler.set_token = set_token

        self.handler(self.message)

        self.executor.assert_called_once_with(self.message.command_line,
                net_key=self.net_key, response_places=self.response_places,
                passthru=True)

        expected = [
                mock.call(self.net_key, self.pre_dispatch_place_idx),
                mock.call(self.net_key, self.dispatch_success_place_idx,
                        data={'pid': str(job_id)})
                ]

        self.assertEqual(expected, set_token.mock_calls)

    def test_message_handler_executor_failure(self):
        job_id = 1234
        self.executor.return_value = (job_id, False)

        set_token = mock.Mock()
        self.handler.set_token = set_token

        self.handler(self.message)

        self.executor.assert_called_once_with(self.message.command_line,
                net_key=self.net_key, response_places=self.response_places,
                passthru=True)

        self.assertEqual([mock.call(self.net_key, self.pre_dispatch_place_idx),
                          mock.call(self.net_key, self.dispatch_failure_place_idx,
                              data={'pid': str(job_id)})],
                         set_token.mock_calls)



    def test_message_handler_executor_exception(self):
        self.executor.side_effect = RuntimeError

        with mock.patch("flow.command_runner.handler.Token") as T:
            T.create = mock.Mock()
            token = mock.Mock
            token.key = mock.Mock(str)
            T.create.return_value = token

            self.assertRaises(RuntimeError, self.handler, self.message)

            self.executor.assert_called_once_with(self.message.command_line,
                    net_key=self.net_key, response_places=self.response_places,
                    passthru=True)


if '__main__' == __name__:
    unittest.main()
