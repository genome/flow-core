#!/usr/bin/env python

# This is taken from Stack Overflow:
#   http://stackoverflow.com/questions/1191374/subprocess-with-timeout

import os
from signal import alarm, signal, SIGALRM, SIGKILL
from subprocess import PIPE, Popen

import sys


def run(args, cwd = None, shell = False, kill_tree = True, timeout = -1,
        env = None):
    '''
    Run a command with a timeout after which it will be forcibly
    killed.
    '''
    class Alarm(Exception):
        pass
    def alarm_handler(signum, frame):
        raise Alarm
    p = Popen(args, shell = shell, cwd = cwd, stdout = PIPE, stderr = PIPE,
            env = env)
    if timeout != -1:
        signal(SIGALRM, alarm_handler)
        alarm(timeout)
    try:
        stdout, stderr = p.communicate()
        if timeout != -1:
            alarm(0)
    except Alarm:
        pids = [p.pid]
        if kill_tree:
            pids.extend(get_process_children(p.pid))
        for pid in pids:
            # process might have died before getting to this line
            # so wrap to avoid OSError: no such process
            try:
                os.kill(pid, SIGKILL)
            except OSError:
                pass
        return -9, '', ''
    return p.returncode, stdout, stderr


def get_process_children(pid):
    p = Popen('ps --no-headers -o pid --ppid %d' % pid, shell = True,
              stdout = PIPE, stderr = PIPE)
    stdout, stderr = p.communicate()
    return [int(p) for p in stdout.split()]


def config_path():
    return os.path.join(os.path.dirname(__file__), 'local-redis.conf')


def main():
    return run(['redis-server', config_path()] + sys.argv[1:], timeout=2400)


if __name__ == '__main__':
    main()
