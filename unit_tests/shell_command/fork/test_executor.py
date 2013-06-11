from flow import exit_codes
from flow.shell_command.fork import executor

import mock
import tempfile
import unittest


class ForkExecutorTest(unittest.TestCase):
    def setUp(self):
        self.executor = executor.ForkExecutor(
                default_environment={}, mandatory_environment={})

    def test_succeeded_job(self):
        job_id_callback = mock.Mock()

        rv = self.executor.execute_command_line(
                job_id_callback, ['/bin/true'], {}, {})

        self.assertEqual(exit_codes.EXECUTE_SUCCESS, rv)
        job_id_callback.assert_called_once_with(mock.ANY)

    def test_failed_job(self):
        job_id_callback = mock.Mock()

        rv = self.executor.execute_command_line(
                job_id_callback, ['/bin/false'], {}, {})

        self.assertEqual(exit_codes.EXECUTE_FAILURE, rv)
        job_id_callback.assert_called_once_with(mock.ANY)

    def test_negative_exit(self):
        job_id_callback = mock.Mock()

        rv = self.executor.execute_command_line(
                job_id_callback, ['/bin/bash', '-c', 'kill -9 $$'], {}, {})

        self.assertEqual(exit_codes.EXECUTE_ERROR, rv)
        job_id_callback.assert_called_once_with(mock.ANY)


    def test_stdout(self):
        job_id_callback = mock.Mock()

        stdin = tempfile.NamedTemporaryFile()
        stdout = tempfile.NamedTemporaryFile()

        text = 'just some sample text'
        stdin.write(text)
        stdin.flush()

        executor_data = {
            'stdin': stdin.name,
            'stdout': stdout.name,
        }

        rv = self.executor.execute_command_line(
                job_id_callback, ['/bin/cat'], executor_data, {})

        job_id_callback.assert_called_once_with(mock.ANY)

        self.assertEqual(text, stdout.read())

    def test_stderr(self):
        job_id_callback = mock.Mock()

        stderr = tempfile.NamedTemporaryFile()
        stdin = tempfile.NamedTemporaryFile()

        text = 'just some sample text'
        stdin.write(text)
        stdin.flush()

        executor_data = {
            'stderr': stderr.name,
            'stdin': stdin.name,
        }

        rv = self.executor.execute_command_line(
                job_id_callback, ['bash', '-c', '/bin/cat 1>&2'], executor_data, {})

        job_id_callback.assert_called_once_with(mock.ANY)

        self.assertEqual(text, stderr.read())

    def test_on_job_id(self):
        job_id = mock.Mock()
        callback_data = mock.Mock()
        service_interfaces = mock.Mock()

        send_message = mock.Mock()
        with mock.patch('flow.shell_command.fork.executor.send_message',
                new=send_message):
            self.executor.on_job_id(job_id, callback_data, service_interfaces)

        send_message.assert_any_call(
                'msg: dispatch_success', callback_data, service_interfaces,
                token_data={'job_id': job_id})

        send_message.assert_any_call(
                'msg: execute_begin', callback_data, service_interfaces,
                token_data={'hostname': mock.ANY})

    def test_on_failure(self):
        exit_code = mock.Mock()
        callback_data = mock.Mock()
        service_interfaces = mock.Mock()

        send_message = mock.Mock()
        with mock.patch('flow.shell_command.fork.executor.send_message',
                new=send_message):
            self.executor.on_failure(exit_code, callback_data, service_interfaces)

        send_message.assert_any_call(
                'msg: execute_failure', callback_data, service_interfaces,
                token_data={'exit_code': exit_code})

    def test_on_failure(self):
        callback_data = mock.Mock()
        service_interfaces = mock.Mock()

        send_message = mock.Mock()
        with mock.patch('flow.shell_command.fork.executor.send_message',
                new=send_message):
            self.executor.on_success(callback_data, service_interfaces)

        send_message.assert_any_call(
                'msg: execute_success', callback_data, service_interfaces)


if '__main__' == __name__:
    unittest.main()
