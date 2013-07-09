from flow import exit_codes
from flow.shell_command import executor_base
from flow.shell_command.execution_environment import *
from flow.util import environment as env_util

import mock
import unittest


class ExecutionEnvironmentTest(unittest.TestCase):
    def setUp(self):
        self.group_id = mock.Mock()
        self.user_id = mock.Mock()
        self.working_directory = mock.Mock()
        self.environment = {
            'sample': 'env',
        }

        self.execution_environment = ExecutionEnvironment(
                group_id=self.group_id, user_id=self.user_id,
                working_directory=self.working_directory,
                environment=self.environment)

    def test_enter_OK(self):
        with mock.patch('flow.shell_command.execution_environment.os') as os:
            with env_util.environment({}):
                self.execution_environment.enter()

            os.setgid.assert_called_once_with(self.group_id)
            os.setuid.assert_called_once_with(self.user_id)

            os.chdir.assert_called_once_with(self.working_directory)

            os.environ.clear.assert_called_once_with()
            os.environ.update.assert_called_once_with(self.environment)

    def test_enter_chdir_error(self):
        with mock.patch('flow.shell_command.execution_environment.os') as os:
            os.chdir.side_effect = OSError
            with mock.patch('flow.shell_command.execution_environment'
                            '.exit_process') as ep:
                self.execution_environment.enter()
                ep.assert_called_once_with(exit_codes.EXECUTE_SYSTEM_FAILURE)

    def test_enter_setgid_error(self):
        with mock.patch('flow.shell_command.execution_environment.os') as os:
            os.setgid.side_effect = OSError
            with mock.patch('flow.shell_command.execution_environment'
                            '.exit_process') as ep:
                self.execution_environment.enter()
                ep.assert_called_once_with(exit_codes.EXECUTE_SYSTEM_FAILURE)

    def test_enter_setuid_error(self):
        with mock.patch('flow.shell_command.execution_environment.os') as os:
            os.setuid.side_effect = OSError
            with mock.patch('flow.shell_command.execution_environment'
                            '.exit_process') as ep:
                self.execution_environment.enter()
                ep.assert_called_once_with(exit_codes.EXECUTE_SYSTEM_FAILURE)


if '__main__' == __name__:
    unittest.main()