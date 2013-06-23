from flow import exit_codes
from flow.shell_command import executor_base
from flow.shell_command.execution_environment import *
from twisted.internet import defer

import mock
import unittest

class TestExecutorBase(executor_base.ExecutorBase):
    def __init__(self, *args, **kwargs):
        executor_base.ExecutorBase.__init__(self, *args, **kwargs)

        self.job_id = None
        self.success = None
        self.failure_exit_code = None
        self.signal_number = None

    def on_job_id(self, job_id, callback_data, service_interfaces):
        self.job_id = job_id
        return defer.succeed(None)

    def on_failure(self, exit_code, callback_data, service_interfaces):
        self.failure_exit_code = exit_code
        return defer.succeed(None)

    def on_signal(self, signal_number, callback_data, service_interfaces):
        self.signal_number = signal_number
        return defer.succeed(None)

    def on_success(self, callback_data, service_interfaces):
        self.success = True
        return defer.succeed(None)


class SucceedingExecutor(TestExecutorBase):
    def execute_command_line(self, job_id_callback, command_line,
            executor_data, resources):
        job_id_callback(executor_data['set_job_id'])
        return exit_codes.EXECUTE_SUCCESS

class FailingExecutor(TestExecutorBase):
    def execute_command_line(self, job_id_callback, command_line,
            executor_data, resources):
        return exit_codes.EXECUTE_FAILURE

class ErrorExecutor(TestExecutorBase):
    def execute_command_line(self, job_id_callback, command_line,
            executor_data, resources):
        raise RuntimeError('Always raise exception')


class ExecutorBaseTest(unittest.TestCase):
    def test_success(self):
        e = SucceedingExecutor(default_environment={}, mandatory_environment={})
        set_job_id = '42'
        with mock.patch('flow.shell_command.util.set_gid_and_uid_or_exit'):
            deferred = e.execute(
                    execution_environment=NullExecutionEnvironment(),
                    command_line=['a', 'b', 'c'],
                    executor_data={'set_job_id': set_job_id}, callback_data={},
                    resources={}, service_interfaces={})

        self.assertEqual(set_job_id, e.job_id)
        self.assertTrue(e.success)

    def test_failure(self):
        e = FailingExecutor(default_environment={}, mandatory_environment={})
        with mock.patch('flow.shell_command.util.set_gid_and_uid_or_exit'):
            deferred = e.execute(
                    execution_environment=NullExecutionEnvironment(),
                    command_line=['a', 'b', 'c'],
                    executor_data={}, callback_data={}, resources={},
                    service_interfaces={})

        self.assertEqual(exit_codes.EXECUTE_FAILURE, e.failure_exit_code)

    def test_error(self):
        e = ErrorExecutor(default_environment={}, mandatory_environment={})

        with mock.patch('flow.shell_command.util.set_gid_and_uid_or_exit'):
            deferred = e.execute(
                    execution_environment=NullExecutionEnvironment(),
                    command_line=['a', 'b', 'c'],
                    executor_data={}, callback_data={}, resources={},
                    service_interfaces={})

        self.assertEqual(9, e.signal_number)


    def test_child(self):
        e = SucceedingExecutor(default_environment={}, mandatory_environment={})

        socket = mock.Mock()

        group_id = mock.Mock()
        user_id = mock.Mock()
        working_directory = mock.Mock()
        environment = {
            'sample': 'env',
        }

        execution_environment = ExecutionEnvironment(
                group_id=group_id, user_id=user_id,
                working_directory=working_directory,
                environment=environment)

        command_line = mock.Mock()
        executor_data = {
            'set_job_id': 'awesome job id',
        }
        resources = mock.Mock()

        chdir = mock.Mock()
        set_gid_and_uid_or_exit = mock.Mock()
        with mock.patch(
                'flow.shell_command.util.set_gid_and_uid_or_exit',
                new=set_gid_and_uid_or_exit):
            with mock.patch('os.chdir', new=chdir):
                exit_code = e._child(socket, execution_environment,
                        command_line, executor_data, resources)

        set_gid_and_uid_or_exit.assert_called_once_with(group_id, user_id)
        chdir.assert_called_once_with(working_directory)

        socket.send.assert_called_once_with(executor_data['set_job_id'])
        socket.close.assert_called_once_with()


class SendMessageTest(unittest.TestCase):
    def test_send_message(self):
        service_interfaces = {
            'orchestrator': mock.Mock(),
        }
        args = {
            'net_key': mock.Mock(),
            'color': mock.Mock(),
            'color_group_idx': mock.Mock(),
        }
        callback_data = {
            'my_place_name': mock.Mock(),
        }
        callback_data.update(args)

        token_data = mock.Mock()

        executor_base.send_message('my_place_name', callback_data=callback_data,
                service_interfaces=service_interfaces, token_data=token_data)

        service_interfaces['orchestrator'].create_token.assert_called_once_with(
                place_idx=callback_data['my_place_name'], data=token_data,
                **args)



if '__main__' == __name__:
    unittest.main()
