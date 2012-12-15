import unittest
try:
    from unittest import mock
except:
    import mock

from amqp_service.dispatcher import lsf
from pythonlsf import lsf as lsf_driver


class SetCommandStringTest(unittest.TestCase):
    def setUp(self):
        self.command = 'command'
        self.arguments = ['c1', 'c2']
        self.wrapper = 'wrapper'
        self.wrapper_arguments = ['w1', 'w2']

        self.request = mock.Mock()
        self.request.options = 0

    def test_command_only(self):
        command_string = lsf.set_command_string(self.request, self.command)
        self.assertEqual('command', command_string)
        self.assertEqual(self.request.options, 0)

    def test_command_and_args(self):
        command_string = lsf.set_command_string(self.request, self.command,
                arguments=self.arguments)
        self.assertEqual('command c1 c2', command_string)
        self.assertEqual(self.request.options, 0)

    def test_wrapper_no_w_args(self):
        command_string = lsf.set_command_string(self.request, self.command,
                arguments=self.arguments, wrapper=self.wrapper)
        self.assertEqual('wrapper command c1 c2', command_string)
        self.assertEqual(self.request.options, lsf_driver.SUB_JOB_NAME)
        self.assertEqual(self.request.jobName, self.command)

    def test_no_wrapper_but_w_args(self):
        command_string = lsf.set_command_string(self.request, self.command,
                arguments=self.arguments,
                wrapper_arguments=self.wrapper_arguments)
        self.assertEqual('command c1 c2', command_string)
        self.assertEqual(self.request.options, 0)


    def test_wrapper_and_w_args(self):
        command_string = lsf.set_command_string(self.request, self.command,
                arguments=self.arguments, wrapper=self.wrapper,
                wrapper_arguments=self.wrapper_arguments)
        self.assertEqual('wrapper w1 w2 command c1 c2', command_string)
        self.assertEqual(self.request.options, lsf_driver.SUB_JOB_NAME)
        self.assertEqual(self.request.jobName, self.command)


class GetRlimitsTest(unittest.TestCase):
    AVAILABLE_RLIMITS = [
            ('max_resident_memory', lsf_driver.LSF_RLIMIT_RSS),
            ('max_virtual_memory', lsf_driver.LSF_RLIMIT_VMEM),
            ('max_processes', lsf_driver.LSF_RLIMIT_PROCESS),
            ('max_threads', lsf_driver.LSF_RLIMIT_THREAD),
            ('max_open_files', lsf_driver.LSF_RLIMIT_NOFILE),
            ('max_stack_size', lsf_driver.LSF_RLIMIT_STACK)
    ]

    def simple_rlim_success(self, name, index, value=42):
        kwargs = {name: value}
        expected_limits = create_expected_limits()
        expected_limits[index] = value
        limits = lsf.get_rlimits(**kwargs)
        self.assertEqual(limits, expected_limits)

    def simple_rlim_failure(self, name):
        kwargs = {name: mock.Mock()}
        self.assertRaises(TypeError, lsf.get_rlimits, **kwargs)


    def test_defaults(self):
        expected_limits = create_expected_limits()
        limits = lsf.get_rlimits()
        self.assertEqual(limits, expected_limits)


    def test_all_success(self):
        for name, index in self.AVAILABLE_RLIMITS:
            self.simple_rlim_success(name, index)

    def test_all_failure(self):
        for name, index in self.AVAILABLE_RLIMITS:
            self.simple_rlim_failure(name)

def create_expected_limits():
    return [lsf_driver.DEFAULT_RLIMIT
            for i in xrange(lsf_driver.LSF_RLIM_NLIMITS)]


class CreateRequestTest(unittest.TestCase):
    def setUp(self):
        self.default_queue = 'serious queue'
        self.dispatcher = lsf.LSFDispatcher(default_queue=self.default_queue)

        self.command = 'complex command'

    def test_command_only_success(self):
        request = self.dispatcher.create_request(self.command)
        self.assertEqual(request.command, self.command)
        self.assertEqual(request.queue, self.default_queue)

    def test_command_only_failure(self):
        self.assertRaises(TypeError,
                self.dispatcher.create_request, mock.Mock())


    def test_queue_success(self):
        queue = 'different queue'
        request = self.dispatcher.create_request(self.command, queue=queue)
        self.assertEqual(request.command, self.command)
        self.assertEqual(request.queue, queue)

    def test_queue_failure(self):
        self.assertRaises(TypeError,
                self.dispatcher.create_request, self.command, queue=mock.Mock())


    def test_stdout_success(self):
        stdout = 'stdout path'
        request = self.dispatcher.create_request(self.command, stdout=stdout)
        self.assertEqual(request.command, self.command)
        self.assertEqual(request.queue, self.default_queue)
        self.assertEqual(request.outFile, stdout)

    def test_stdout_failure(self):
        self.assertRaises(TypeError,
                self.dispatcher.create_request, self.command,
                stdout=mock.Mock())


    def test_stderr_success(self):
        stderr = 'stderr path'
        request = self.dispatcher.create_request(self.command, stderr=stderr)
        self.assertEqual(request.command, self.command)
        self.assertEqual(request.queue, self.default_queue)
        self.assertEqual(request.errFile, stderr)

    def test_stderr_failure(self):
        self.assertRaises(TypeError,
                self.dispatcher.create_request, self.command,
                stderr=mock.Mock())


    def test_rlimits(self):
        value = 4000
        request = self.dispatcher.create_request(self.command,
                max_resident_memory=value)
        expected_limits = create_expected_limits()
        expected_limits[lsf_driver.LSF_RLIMIT_RSS] = value
        self.assertEqual(request.command, self.command)
        self.assertEqual(request.queue, self.default_queue)

        for i, x in enumerate(expected_limits):
            self.assertEqual(request.rLimits[i], x)



if '__main__' == __name__:
    unittest.main()
