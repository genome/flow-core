from flow import exit_codes
from flow.shell_command.lsf.commands import post_exec
from flow.util.environment import environment

import mock
import unittest


class PostExecCommandTest(unittest.TestCase):
    def test_execute_success(self):
        orchestrator = mock.Mock()
        command = post_exec.LsfPostExecCommand(orchestrator=orchestrator)

        parsed_arguments = mock.Mock()

        exit_code = 0
        signal = 0
        env = {
            'LSB_JOBEXIT_STAT': make_lsf_jobexit_stat(exit_code, signal)
        }

        with environment([env]):
            command._execute(parsed_arguments)

        orchestrator.create_token.assert_called_once_with(
                net_key=parsed_arguments.net_key,
                place_idx=parsed_arguments.execute_success,
                color=parsed_arguments.color,
                color_group_idx=parsed_arguments.color_group_idx)


    def test_execute_failure_exit_code(self):
        orchestrator = mock.Mock()
        command = post_exec.LsfPostExecCommand(orchestrator=orchestrator)

        parsed_arguments = mock.Mock()

        exit_code = 3
        signal = 0
        env = {
            'LSB_JOBEXIT_STAT': make_lsf_jobexit_stat(exit_code, signal)
        }

        with environment([env]):
            command._execute(parsed_arguments)

        data = {
            'exit_code': exit_code,
            'signal_number': signal,
        }
        orchestrator.create_token.assert_called_once_with(
                net_key=parsed_arguments.net_key,
                place_idx=parsed_arguments.execute_failure,
                color=parsed_arguments.color,
                color_group_idx=parsed_arguments.color_group_idx,
                data=data)

    def test_execute_failure_signal(self):
        orchestrator = mock.Mock()
        command = post_exec.LsfPostExecCommand(orchestrator=orchestrator)

        parsed_arguments = mock.Mock()

        exit_code = 0
        signal = 5
        env = {
            'LSB_JOBEXIT_STAT': make_lsf_jobexit_stat(exit_code, signal)
        }

        with environment([env]):
            command._execute(parsed_arguments)

        data = {
            'exit_code': exit_code,
            'signal_number': signal,
        }
        orchestrator.create_token.assert_called_once_with(
                net_key=parsed_arguments.net_key,
                place_idx=parsed_arguments.execute_failure,
                color=parsed_arguments.color,
                color_group_idx=parsed_arguments.color_group_idx,
                data=data)

    def test_execute_no_jobexit_stat(self):
        orchestrator = mock.Mock()
        command = post_exec.LsfPostExecCommand(orchestrator=orchestrator)

        parsed_arguments = mock.Mock()

        exit_process = mock.Mock()
        exit_process.side_effect = RuntimeError
        with mock.patch(
                'flow.shell_command.lsf.commands.post_exec.exit_process',
                new=exit_process):
            with self.assertRaises(RuntimeError):
                command._execute(parsed_arguments)

        self.assertFalse(orchestrator.create_token.called)
        exit_process.assert_called_once_with(exit_codes.EXECUTE_ERROR)


def make_lsf_jobexit_stat(exit_code, signal):
    return str((exit_code << 8) + signal)


if '__main__' == __name__:
    unittest.main()
