#!/usr/bin/env python

from flow.shell_command import executor_utils
import sys


def main():
    args = executor_utils.parse_arguments()

    executor_utils.write_job_id(args.job_id_fd, sys.stdin.read())


if __name__ == '__main__':
    main()
