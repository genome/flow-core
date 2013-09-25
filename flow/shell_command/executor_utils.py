import argparse
import os
import sys


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('--job_id_fd', type=int,
            help='File descriptor to write job_id to')

    return parser.parse_args()


def write_job_id(fd, pid):
    if fd is not None:
        job_file = os.fdopen(fd, 'w')
    else:
        job_file = sys.stdout

    job_file.write(str(pid))
    job_file.close()
