from Queue import Queue, Empty

import flow.util.exit
import mock
import os
import psutil
import signal
import socket
import subprocess
import time
import unittest


def signal_safe_sleep(delay):
    q = Queue()
    try:
        q.get(True, delay)
    except Empty:
        pass


def child(parent_socket, child_socket, exit_on_signal=False):
    parent_socket.close()

    def _handler(signum, frame):
        child_socket.send("%d\n" % signum)
        if exit_on_signal:
            os._exit(1)

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)

    signal_safe_sleep(60)

    os._exit(1)


class TestExitProcess(unittest.TestCase):
    def test_obedient_child(self):
        self.verify_simple_child(exit_on_signal=True,
                    expected_signals=[signal.SIGINT])

    def test_persistent_child(self):
        self.verify_simple_child(exit_on_signal=False,
                    expected_signals=[signal.SIGINT, signal.SIGTERM])


    def verify_simple_child(self, exit_on_signal, expected_signals):
        parent_socket, child_socket = socket.socketpair()
        child_pid = os.fork()
        if child_pid == 0:
            child(parent_socket, child_socket,
                    exit_on_signal=exit_on_signal)
        else:
            self.parent_send_signal(child_pid, parent_socket, child_socket,
                    expected_signals=expected_signals)

    def parent_send_signal(self, child_pid, parent_socket, child_socket,
            expected_signals):
        child_socket.close()
        time.sleep(1)
        with mock.patch('os._exit') as _exit:
            with mock.patch('flow.util.exit._SIGNAL_TIMEOUT', 1):
                flow.util.exit.exit_process(1)

        lines = parent_socket.recv(256).splitlines()
        self.assertEqual(expected_signals, map(int, lines))

        self.assertEqual([], psutil.Process(os.getpid()).get_children())


if __name__ == '__main__':
    unittest.main()
