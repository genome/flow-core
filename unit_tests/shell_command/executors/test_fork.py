from flow.shell_command.executors import fork

import mock
import sys
import unittest


success_script = "import sys; sys.exit(0)"
failure_script = "import sys; sys.exit(1)"


class ForkExecutorTest(unittest.TestCase):
    def setUp(self):
        self.dispatcher = fork.ForkExecutor(wrapper=[],
                default_environment={}, mandatory_environment={})
        self.response_places = {
            'begin_execute': '0',
            'execute_success': '1',
            'execute_failure': '2',
        }

    def test_succeeded_job(self):
        self.dispatcher.wrapper = [sys.executable, '-c', success_script, '--']

        success, result = self.dispatcher.execute(['/bin/true'],
                net_key="x",
                response_places=self.response_places)
        self.assertTrue(success)
        self.assertEqual(result, 0)

    def test_failed_job(self):
        self.dispatcher.wrapper = [sys.executable, '-c', failure_script, '--']

        success, result = self.dispatcher.execute(['/bin/false'],
                net_key="x",
                response_places=self.response_places)
        self.assertFalse(success)
        self.assertEqual(result, 1)

    def test_missing_response_places(self):
        self.assertRaises(TypeError,
                self.dispatcher.execute, [mock.Mock()])


if '__main__' == __name__:
    unittest.main()