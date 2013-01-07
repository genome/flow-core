import unittest
try:
    from unittest import mock
except:
    import mock

from flow_command_runner.executors import local


class SubprocessExecutorTest(unittest.TestCase):
    def setUp(self):
        self.dispatcher = local.SubprocessExecutor()

    def test_succeeded_job(self):
        success, result = self.dispatcher(['/bin/true'])
        self.assertTrue(success)
        self.assertEqual(result, 0)

    def test_failed_job(self):
        success, result = self.dispatcher(['/bin/false'])
        self.assertFalse(success)
        self.assertEqual(result, 1)

    def test_error(self):
        self.assertRaises(RuntimeError,
                self.dispatcher, [mock.Mock()])


if '__main__' == __name__:
    unittest.main()
