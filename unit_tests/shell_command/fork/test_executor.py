from flow.shell_command.fork import executor

import mock
import unittest


class ForkExecutorTest(unittest.TestCase):
    def setUp(self):
        self.executor = executor.ForkExecutor(
                default_environment={}, mandatory_environment={})

    def test_succeeded_job(self):
        job_id_callback = mock.Mock()

        rv = self.executor.execute_command_line(
                job_id_callback, ['/bin/true'], {})

        self.assertEqual(0, rv)
        job_id_callback.assert_called_once_with(mock.ANY)

    def test_failed_job(self):
        job_id_callback = mock.Mock()

        rv = self.executor.execute_command_line(
                job_id_callback, ['/bin/false'], {})

        self.assertEqual(1, rv)
        job_id_callback.assert_called_once_with(mock.ANY)


if '__main__' == __name__:
    unittest.main()
