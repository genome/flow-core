import os
import subprocess
import unittest

from test_helpers import redistest


EXECUTABLE = os.path.join(os.path.dirname(__file__), 'fork_sc_runner.py')


class ForkShellCommandSystemTest(redistest.RedisTest):
    def test_success(self):
        subprocess.check_call([EXECUTABLE,
            '--redis-socket-path', self.redis_unix_socket_path], close_fds=True)

    def test_failure(self):
        subprocess.check_call([EXECUTABLE, '--expect-failure',
                '--redis-socket-path', self.redis_unix_socket_path])


if __name__ == '__main__':
    unittest.main()
