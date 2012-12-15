import unittest
try:
    from unittest import mock
except:
    import mock

from amqp_service.dispatcher import subprocess_dispatcher


class SubprocessDispatcherTest(unittest.TestCase):
    def setUp(self):
        self.dispatcher = subprocess_dispatcher.SubprocessDispatcher()

    def test_succeeded_job(self):
        success, result = self.dispatcher.launch_job('/bin/true')
        self.assertTrue(success)
        self.assertEqual(result, None)

    def test_failed_job(self):
        success, result = self.dispatcher.launch_job('/bin/false')
        self.assertFalse(success)
        self.assertEqual(result, None)


if '__main__' == __name__:
    unittest.main()
