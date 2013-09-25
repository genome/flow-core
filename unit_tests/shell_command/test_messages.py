from flow.protocol.exceptions import InvalidMessageException
from flow.shell_command.messages import ShellCommandSubmitMessage

import mock
import unittest


class ShellCommandMessagesTest(unittest.TestCase):
    def test_submit_valid(self):
        message = ShellCommandSubmitMessage(
                group_id=100,
                user_id=100,
                environment={'d': 'e', 'f': 'g'},
                executor_data={
                    'command_line': ['a', 'b', 'c'],
                    'umask': 2,
                    'resources': {
                        'limit': {
                            'foo': 'scalar',
                        },
                        'reserve': {
                            'bar': 7,
                        },
                        'request': {
                            'baz': 'buz',
                        },
                    },
                })

        self.assertTrue(message)


    def test_submit_invalid_command_line(self):
        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    executor_data={'command_line': [object(), object()]},
                    group_id=100, user_id=100)

        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    executor_data={'command_line': [7]},
                    group_id=100, user_id=100)

        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    executor_data={'command_line': {'a': 'b'}},
                    group_id=100, user_id=100)

        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    executor_data={'command_line': 'a b c'},
                    group_id=100, user_id=100)


    def test_submit_invalid_environment(self):
        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    executor_data={'command_line': ['a', 'b', 'c']},
                    group_id=100, user_id=100,
                    environment={1: 'badnews'},
            )

        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    executor_data={'command_line': ['a', 'b', 'c']},
                    group_id=100, user_id=100,
                    environment={'badnews': 1},
            )


    def test_submit_invalid_resources(self):
        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    executor_data={
                        'command_line': ['a', 'b', 'c'],
                        'resources': {'bad': 'news'},
                    },
                    group_id=100, user_id=100,
            )

        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    executor_data={
                        'command_line': ['a', 'b', 'c'],
                        'resources': {'limit': 'badnews'},
                    },
                    group_id=100, user_id=100,
            )

        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    executor_data={
                        'command_line': ['a', 'b', 'c'],
                        'resources': {'limit': {1: 'badnews'}},
                    },
                    group_id=100, user_id=100,
            )

        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    executor_data={
                        'command_line': ['a', 'b', 'c'],
                        'resources': {'limit': {'badnews': object()}},
                    },
                    group_id=100, user_id=100,
            )


if '__main__' == __name__:
    unittest.main()
