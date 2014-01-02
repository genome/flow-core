import logging
import os
import psutil
import signal
import time


LOG = logging.getLogger(__name__)


_SIGNAL_TIMEOUT = 10


def exit_process(exit_code, child_signals=[signal.SIGINT, signal.SIGTERM]):
    LOG.info('Exitting process: signalling children.')

    for signum in child_signals:
        _signal_child_processes(signum, timeout=_SIGNAL_TIMEOUT)

    _signal_child_processes(signal.SIGKILL, recursive=True,
            timeout=_SIGNAL_TIMEOUT)

    LOG.info('Children killed, exiting with code %d', exit_code)
    os._exit(exit_code)


def _signal_child_processes(signum, recursive=False, timeout=_SIGNAL_TIMEOUT):
    for child in psutil.Process(os.getpid()).get_children(recursive=recursive):
        child.send_signal(signum)

    _wait_children(timeout, recursive=recursive)


def _wait_children(timeout, recursive=False):
    final_time = time.time() + timeout
    for child in psutil.Process(os.getpid()).get_children(recursive=recursive):
        try:
            child.wait(max(0, final_time - time.time()))
        except psutil.TimeoutExpired:
            break
