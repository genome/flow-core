from flow.shell_command.lsf.commands import pre_exec

import mock
import unittest


class PreExecCommandTest(unittest.TestCase):
    def test_annotate_parser(self):
        orchestrator = mock.Mock()
        command = pre_exec.LsfPreExecCommand(orchestrator=orchestrator)

        parser = mock.Mock()

        command.annotate_parser(parser)

        self.assertEqual(4, len(parser.add_argument.mock_calls))
        parser.add_argument.assert_any_call('--color')
        parser.add_argument.assert_any_call('--color-group-idx')
        parser.add_argument.assert_any_call('--execute-begin', '-b')
        parser.add_argument.assert_any_call('--net-key', '-n')


    def test_execute(self):
        orchestrator = mock.Mock()
        command = pre_exec.LsfPreExecCommand(orchestrator=orchestrator)

        parsed_arguments = mock.Mock()

        hostname = mock.Mock()
        gethostname = mock.Mock()
        gethostname.return_value = hostname

        with mock.patch('socket.gethostname', new=gethostname):
            command._execute(parsed_arguments)

        gethostname.assert_called_once_with()
        orchestrator.create_token.assert_called_once_with(
                net_key=parsed_arguments.net_key,
                place_idx=parsed_arguments.execute_begin,
                color=parsed_arguments.color,
                color_group_idx=parsed_arguments.color_group_idx,
                data={'hostname': hostname})


if '__main__' == __name__:
    unittest.main()
