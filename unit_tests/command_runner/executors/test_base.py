import mock
import sys
import unittest

from flow.command_runner.executor import ExecutorBase
from flow import exit_codes


class SucceedingExecutor(ExecutorBase):
    def execute(self, command_line, job_id=None, **kwargs):
        return True, job_id

class FailingExecutor(ExecutorBase):
    def execute(self, command_line, job_id=None, **kwargs):
        return False, job_id

class ErrorExecutor(ExecutorBase):
    def execute(self, command_line, **kwargs):
        raise RuntimeError('Always raise exception')


class ExecutorBaseTest(unittest.TestCase):
    def test_success(self):
        e = SucceedingExecutor()
        set_job_id = 42
        result_job_id, success = e(['a', 'b', 'c'], job_id=set_job_id)

        self.assertEqual(set_job_id, result_job_id)
        self.assertTrue(success)

    def test_failure(self):
        e = FailingExecutor()
        set_job_id = 42
        result_job_id, success = e(['a', 'b', 'c'], job_id=set_job_id)

        self.assertFalse(success)

    def test_error(self):
        e = ErrorExecutor()
        set_job_id = 42
        self.assertRaises(RuntimeError, e, ['a', 'b', 'c'], job_id=set_job_id)


if '__main__' == __name__:
    unittest.main()
