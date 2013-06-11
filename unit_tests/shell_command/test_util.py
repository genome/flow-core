from flow.shell_command import util
from uuid import uuid4

import mock
import os
import socket
import unittest


class WaitForPidTest(unittest.TestCase):
    def test_success(self):
        pid = os.fork()
        if pid == 0:
            os._exit(0)

        exit_code, signal = util.wait_for_pid(pid)
        self.assertEqual(0, exit_code)
        self.assertEqual(0, signal)

    def test_exit_code(self):
        pid = os.fork()
        if pid == 0:
            os._exit(1)

        exit_code, signal = util.wait_for_pid(pid)
        self.assertEqual(1, exit_code)
        self.assertEqual(0, signal)

    def test_signal(self):
        pid = os.fork()
        if pid == 0:
            os.kill(os.getpid(), 9)

        exit_code, signal = util.wait_for_pid(pid)
        self.assertEqual(9, signal)


class SocketpairTest(unittest.TestCase):
    def test_success(self):
        parent_socket, child_socket = mock.Mock(), mock.Mock()
        socketpair = mock.Mock()
        socketpair.return_value = parent_socket, child_socket
        with mock.patch('socket.socketpair', new=socketpair):
            ps, cs = util.socketpair_or_exit()

        socketpair.assert_called_once_with()
        self.assertEqual(parent_socket, ps)
        self.assertEqual(child_socket, cs)

    def test_error(self):
        socketpair = mock.Mock()
        socketpair.side_effect = socket.error

        exit_process = mock.Mock()
        exit_process.side_effect = RuntimeError
        with mock.patch('socket.socketpair', new=socketpair):
            with mock.patch('flow.shell_command.util.exit_process',
                    new=exit_process):
                with self.assertRaises(RuntimeError):
                    util.socketpair_or_exit()

        socketpair.assert_called_once_with()

class ForkTest(unittest.TestCase):
    def test_success(self):
        expected_pid = mock.Mock()

        fork = mock.Mock()
        fork.return_value = expected_pid

        with mock.patch('os.fork', new=fork):
            pid = util.fork_or_exit()

        fork.assert_called_once_with()
        self.assertEqual(expected_pid, pid)

    def test_error(self):
        fork = mock.Mock()
        fork.side_effect = OSError

        exit_process = mock.Mock()
        exit_process.side_effect = RuntimeError
        with mock.patch('os.fork', new=fork):
            with mock.patch('flow.shell_command.util.exit_process',
                    new=exit_process):
                with self.assertRaises(RuntimeError):
                    util.fork_or_exit()

        fork.assert_called_once_with()


class SetGidUidTest(unittest.TestCase):
    def setUp(self):
        self.setgid = mock.Mock()
        self.setuid = mock.Mock()

    def test_success(self):
        gid = mock.Mock()
        uid = mock.Mock()
        with mock.patch('os.setgid', new=self.setgid):
            with mock.patch('os.setuid', new=self.setuid):
                util.set_gid_and_uid_or_exit(gid, uid)

        self.setgid.assert_called_once_with(gid)
        self.setuid.assert_called_once_with(uid)

    def test_setgid_fail(self):
        gid = mock.Mock()
        uid = mock.Mock()

        self.setgid.side_effect = OSError

        exit_process = mock.Mock()
        exit_process.side_effect = RuntimeError

        with mock.patch('os.setgid', new=self.setgid):
            with mock.patch('os.setuid', new=self.setuid):
                with mock.patch('flow.shell_command.util.exit_process',
                        new=exit_process):
                    with self.assertRaises(RuntimeError):
                        util.set_gid_and_uid_or_exit(gid, uid)

        self.setgid.assert_called_once_with(gid)

    def test_setuid_fail(self):
        gid = mock.Mock()
        uid = mock.Mock()

        self.setuid.side_effect = OSError

        exit_process = mock.Mock()
        exit_process.side_effect = RuntimeError

        with mock.patch('os.setgid', new=self.setgid):
            with mock.patch('os.setuid', new=self.setuid):
                with mock.patch('flow.shell_command.util.exit_process',
                        new=exit_process):
                    with self.assertRaises(RuntimeError):
                        util.set_gid_and_uid_or_exit(gid, uid)

        self.setgid.assert_called_once_with(gid)
        self.setuid.assert_called_once_with(uid)


if '__main__' == __name__:
    unittest.main()
