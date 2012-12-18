import unittest
try:
    from unittest import mock
except:
    import mock

from amqp_service.dispatcher import lsf
from pythonlsf import lsf as lsf_driver


class ResolveCommandStringTest(unittest.TestCase):
    def setUp(self):
        self.command = 'command'
        self.arguments = ['c1', 'c2']

    def test_command_only(self):
        command_string = lsf.resolve_command_string(self.command)
        self.assertEqual('command', command_string)

    def test_command_and_args(self):
        command_string = lsf.resolve_command_string(self.command,
                arguments=self.arguments)
        self.assertEqual('command c1 c2', command_string)


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

        self.bad_type = mock.Mock()
        self.bad_type.__str__ = lambda x: None


    def test_name_success(self):
        name = 'different name'
        request = self.dispatcher.create_request(name=name)
        self.assertEqual(request.jobName, name)
        self.assertEqual(request.options,
                lsf_driver.SUB_JOB_NAME + lsf_driver.SUB_QUEUE)

    def test_name_failure(self):
        self.assertRaises(TypeError,
                self.dispatcher.create_request, name=self.bad_type)


    def test_queue_success(self):
        queue = 'different queue'
        request = self.dispatcher.create_request(queue=queue)
        self.assertEqual(request.queue, queue)
        self.assertEqual(request.options, lsf_driver.SUB_QUEUE)

    def test_queue_failure(self):
        self.assertRaises(TypeError,
                self.dispatcher.create_request, queue=self.bad_type)


    def test_stdout_success(self):
        stdout = 'stdout path'
        request = self.dispatcher.create_request(stdout=stdout)
        self.assertEqual(request.queue, self.default_queue)
        self.assertEqual(request.outFile, stdout)
        self.assertEqual(request.options,
                lsf_driver.SUB_QUEUE + lsf_driver.SUB_OUT_FILE)

    def test_stdout_failure(self):
        self.assertRaises(TypeError,
                self.dispatcher.create_request, stdout=self.bad_type)


    def test_stderr_success(self):
        stderr = 'stderr path'
        request = self.dispatcher.create_request(stderr=stderr)
        self.assertEqual(request.queue, self.default_queue)
        self.assertEqual(request.errFile, stderr)
        self.assertEqual(request.options,
                lsf_driver.SUB_QUEUE + lsf_driver.SUB_ERR_FILE)

    def test_stderr_failure(self):
        self.assertRaises(TypeError,
                self.dispatcher.create_request, stderr=self.bad_type)


    def test_rlimits(self):
        value = 4000
        request = self.dispatcher.create_request(max_resident_memory=value)
        expected_limits = create_expected_limits()
        expected_limits[lsf_driver.LSF_RLIMIT_RSS] = value
        self.assertEqual(request.queue, self.default_queue)

        for i, x in enumerate(expected_limits):
            self.assertEqual(request.rLimits[i], x)



if '__main__' == __name__:
    unittest.main()
