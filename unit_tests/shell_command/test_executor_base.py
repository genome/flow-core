from flow import exit_codes
from flow.shell_command.executor_base import ExecutorBase
from twisted.internet import defer

import mock
import unittest

class TestExecutorBase(ExecutorBase):
    def __init__(self, *args, **kwargs):
        ExecutorBase.__init__(self, *args, **kwargs)

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
            executor_data):
        job_id_callback(executor_data['set_job_id'])
        return exit_codes.EXECUTE_SUCCESS

class FailingExecutor(TestExecutorBase):
    def execute_command_line(self, job_id_callback, command_line,
            executor_data):
        return exit_codes.EXECUTE_FAILURE

class ErrorExecutor(TestExecutorBase):
    def execute_command_line(self, job_id_callback, command_line,
            executor_data):
        raise RuntimeError('Always raise exception')


class ExecutorBaseTest(unittest.TestCase):
    def test_success(self):
        e = SucceedingExecutor(default_environment={}, mandatory_environment={})
        set_job_id = '42'
        deferred = e.execute(group_id=None, user_id=None, environment={},
                working_directory='/tmp', command_line=['a', 'b', 'c'],
                executor_data={'set_job_id': set_job_id}, callback_data={},
                service_interfaces={})

        self.assertEqual(set_job_id, e.job_id)
        self.assertTrue(e.success)

    def test_failure(self):
        e = FailingExecutor(default_environment={}, mandatory_environment={})
        deferred = e.execute(group_id=None, user_id=None, environment={},
                working_directory='/tmp', command_line=['a', 'b', 'c'],
                executor_data={}, callback_data={}, service_interfaces={})

        self.assertEqual(exit_codes.EXECUTE_FAILURE, e.failure_exit_code)

    def test_error(self):
        e = ErrorExecutor(default_environment={}, mandatory_environment={})

        deferred = e.execute(group_id=None, user_id=None, environment={},
                working_directory='/tmp', command_line=['a', 'b', 'c'],
                executor_data={}, callback_data={}, service_interfaces={})

        self.assertEqual(9, e.signal_number)


if '__main__' == __name__:
    unittest.main()
