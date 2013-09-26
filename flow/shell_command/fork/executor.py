#!/usr/bin/env python

from flow.util.mkdir import make_path_to
from flow.util.signal_handlers import setup_standard_signal_handlers
from flow.shell_command import executor_utils

import contextlib
import json
import os
import subprocess
import sys


def main():
    args = executor_utils.parse_arguments()

    data = json.load(sys.stdin)

    if 'umask' in data:
        os.umask(data['umask'])

    setup_standard_signal_handlers()

    with open_files(data) as (stderr, stdin, stdout):
        p = subprocess.Popen(data['command_line'], close_fds=True,
                stderr=stderr, stdin=stdin, stdout=stdout)

        executor_utils.write_job_id(args.job_id_fd, p.pid)
        sys.exit(p.wait())


@contextlib.contextmanager
def open_files(executor_data):
    stderr = executor_data.get('stderr')
    stdin = executor_data.get('stdin')
    stdout = executor_data.get('stdout')

    make_path_to(stderr)
    make_path_to(stdout)
    make_path_to(stdin)

    stderr_fh = None
    stdin_fh = None
    stdout_fh = None
    try:
        if stderr:
            stderr_fh = open(stderr, 'a')
        if stdin:
            stdin_fh = open(stdin, 'r')
        if stdout:
            stdout_fh = open(stdout, 'a')
        else:
            stdout_fh = sys.stderr

        yield stderr_fh, stdin_fh, stdout_fh

    finally:
        if stderr_fh:
            stderr_fh.close()
        if stdin_fh:
            stdin_fh.close()
        if stdout_fh:
            stdout_fh.close()


if __name__ == '__main__':
    main()
