from flow import exit_codes
from flow.shell_command.lsf import executor
from pythonlsf import lsf

import mock
import os
import unittest


def lsf_available():
    return 'LSF_BINDIR' in os.environ


@unittest.skipIf(not lsf_available(), 'LSF not available')
class LSFExecutorSubmitTest(unittest.TestCase):
    def setUp(self):
        opt_defs = {
            'queue': {
                'name': 'queue',
                'flag': 'SUB_QUEUE',
            },
        }

        self.e = executor.LSFExecutor(pre_exec=None, post_exec=None,
                    option_definitions=opt_defs, default_options={},
                    resource_definitions={})

        self.job_id_callback = mock.Mock()

    def test_submit_success(self):
        executor_data = {}
        resources = {}
        command_line = [u'ls']
        exit_code = self.e.execute_command_line(
                job_id_callback=self.job_id_callback,
                command_line=command_line,
                executor_data=executor_data,
                resources=resources)

        self.assertEqual(exit_codes.EXECUTE_SUCCESS, exit_code)
        self.job_id_callback.assert_called_once_with(mock.ANY)

    def test_submit_failure_illegal_queue(self):
        executor_data = {'lsf_options': {'queue': 'UNKNOWNQUEUE'}}
        resources = {}
        command_line = ['ls']
        exit_code = self.e.execute_command_line(
                job_id_callback=self.job_id_callback,
                command_line=command_line,
                executor_data=executor_data,
                resources=resources)

        self.assertFalse(self.job_id_callback.mock_calls)
        self.assertEqual(exit_codes.EXECUTE_FAILURE, exit_code)




if '__main__' == __name__:
    unittest.main()
