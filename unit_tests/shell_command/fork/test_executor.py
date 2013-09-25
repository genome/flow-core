from StringIO import StringIO

import flow.shell_command.fork.executor
import json
import mock
import os
import subprocess
import tempfile
import unittest


class ForkExecutorTest(unittest.TestCase):
    def executable(self):
        return [os.path.join(os.path.dirname(
            flow.shell_command.fork.executor.__file__), 'executor.py')]

    def test_simple_echo(self):
        stdout = tempfile.NamedTemporaryFile()
        data = {
            'command_line': ['echo', 'bye'],
            'stdout': stdout.name,
        }

        stdin = tempfile.NamedTemporaryFile(delete=False)
        json.dump(data, stdin)
        stdin.close()

        pid = subprocess.check_output(self.executable(),
                stdin=open(stdin.name))

        int(pid)  # Assert that the pid is valid

        self.assertEqual('bye\n', stdout.read())


    def test_false(self):
        stdout = tempfile.NamedTemporaryFile()
        data = {
            'command_line': ['false'],
            'stdout': stdout.name,
        }

        stdin = tempfile.NamedTemporaryFile(delete=False)
        json.dump(data, stdin)
        stdin.close()

        self.assertRaises(subprocess.CalledProcessError,
                subprocess.check_output,
                self.executable(), stdin=open(stdin.name))


if '__main__' == __name__:
    unittest.main()
