#!/usr/bin/env python

from flow import exit_codes
from flow.configuration import defaults
from flow.configuration.inject.initialize import initialize_injector
from flow.configuration.settings.load import load_settings
from flow.shell_command import executor_utils
from flow.shell_command.lsf.request_builder import LSFRequestBuilder
from flow.util.exit import exit_process
from flow.util.signal import setup_standard_exit_handlers
from pythonlsf import lsf

import datetime
import json
import logging.config
import os
import socket
import sys


LOG = logging.getLogger(__name__)


def launch(request):
    reply = create_reply()

    try:
        submit_result = lsf.lsb_submit(request, reply)
    except:
        LOG.exception()
        raise

    if submit_result > 0:
        LOG.debug('Successfully submitted lsf job: %s', submit_result)
        return submit_result

    else:
        lsf.lsb_perror("lsb_submit")
        LOG.error('Failed to submit lsf job, return value = (%s), err = %s',
                submit_result, lsf.lsb_sperror("lsb_submit"))
        exit_process(exit_codes.EXECUTE_FAILURE)


def log_to_user_log_files(executor_data, message):
    template = '%(timestamp)s %(server_host)s: %(message)s\n'

    try:
        combined_message = template % {
                'timestamp': datetime.datetime.now(),
                'server_host': socket.gethostname(),
                'message': message,
        }
        _log_to_file('stderr', executor_data, combined_message)
        _log_to_file('stdout', executor_data, combined_message)

    except:
        LOG.exception('Failed to log to user files: %s', message)


def _log_to_file(param, executor_data, message):
    try:
        if param in executor_data:
            with open(executor_data[param], 'a') as f:
                f.write(message)

    except:
        LOG.exception('Failed to log to user %s: %s', param, message)


def create_reply():
    reply = lsf.submitReply()

    init_code = lsf.lsb_init('')
    if init_code > 0:
        raise RuntimeError("Failed lsb_init, errno = %d" % lsf.lsb_errno())

    return reply


def main():
    args = executor_utils.parse_arguments()

    data = json.load(sys.stdin)

    if 'umask' in data:
        os.umask(data['umask'])

    setup_standard_exit_handlers()

    settings = load_settings('flow-lsf-shell-command-executor',
            parsed_arguments=None)
    logging.config.dictConfig(settings.get('logging',
        defaults.DEFAULT_LOGGING_CONFIG))
    i = initialize_injector(settings)

    request_builder = i.get(LSFRequestBuilder)
    request = request_builder.construct_request(data)

    job_id = launch(request)
    executor_utils.write_job_id(args.job_id_fd, job_id)


if __name__ == '__main__':
    main()
