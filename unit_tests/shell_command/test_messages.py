from flow.protocol.exceptions import InvalidMessageException
from flow.shell_command.messages import ShellCommandSubmitMessage

import mock
import unittest


class ShellCommandMessagesTest(unittest.TestCase):
    def test_submit_valid(self):
        message = ShellCommandSubmitMessage(
                command_line=['a', 'b', 'c'],
                group_id=100,
                user_id=100,
                environment={'d': 'e', 'f': 'g'},
                resources={
                    'limit': {
                        'foo': 'scalar',
                    },
                    'reserve': {
                        'bar': 7,
                    },
                    'request': {
                        'baz': 'buz',
                    },
                })

        self.assertTrue(message)


    def test_submit_invalid_command_line(self):
        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(command_line=[],
                    group_id=100, user_id=100)

        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(command_line=[7],
                    group_id=100, user_id=100)

        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(command_line={'a': 'b'},
                    group_id=100, user_id=100)

        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(command_line='a b c',
                    group_id=100, user_id=100)


    def test_submit_invalid_environment(self):
        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    command_line=['a', 'b', 'c'],
                    group_id=100, user_id=100,
                    environment={1: 'badnews'},
            )

        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    command_line=['a', 'b', 'c'],
                    group_id=100, user_id=100,
                    environment={'badnews': 1},
            )


    def test_submit_invalid_resources(self):
        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    command_line=['a', 'b', 'c'],
                    group_id=100, user_id=100,
                    resources={'bad': 'news'},
            )

        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    command_line=['a', 'b', 'c'],
                    group_id=100, user_id=100,
                    resources={'limit': 'badnews'},
            )

        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    command_line=['a', 'b', 'c'],
                    group_id=100, user_id=100,
                    resources={'limit': {1: 'badnews'}},
            )

        with self.assertRaises(InvalidMessageException):
            ShellCommandSubmitMessage(
                    command_line=['a', 'b', 'c'],
                    group_id=100, user_id=100,
                    resources={'limit': {'badnews': object()}},
            )


if '__main__' == __name__:
    unittest.main()
