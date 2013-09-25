#!/usr/bin/env python

from flow.shell_command.handler_base import ShellCommandSubmitMessageHandler
from flow.shell_command.messages import ShellCommandSubmitMessage
from twisted.internet import defer, reactor

import argparse
import json
import os
import sys


class TestHandler(ShellCommandSubmitMessageHandler):
    def __init__(self, expected_job_id, expect_success, *args, **kwargs):
        self.expected_job_id = expected_job_id
        self.expect_success = expect_success
        ShellCommandSubmitMessageHandler.__init__(self, *args, **kwargs)

    def on_job_id_success(self, job_id, callback_data=None,
            job_id_handled=None):
        if self.expected_job_id != job_id:
            sys.stderr.write('job_id mismatch.  Got (%r), expected (%s)\n'
                    % (job_id, self.expected_job_id))
            os._exit(1)
        else:
            print job_id
            return defer.succeed(job_id)

    def on_job_id_failure(self, error, callback_data=None,
            job_id_handled=None):
        if self.expect_success:
            os._exit(1)
        else:
            reactor.stop()

    def on_job_ended_success(self, result, callback_data=None,
            job_ended_handled=None):
        if self.expect_success:
            reactor.stop()
        else:
            os._exit(1)

    def on_job_ended_failure(self, error, callback_data=None,
            job_ended_handled=None):
        if self.expect_success:
            os._exit(1)
        else:
            reactor.stop()


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('executable')
    parser.add_argument('--expect-success', action='store_true')

    return parser.parse_args()


def main(exe_name, expect_success):
    message = ShellCommandSubmitMessage(
            user_id=os.getuid(), group_id=os.getgid(),
            working_directory='.',
            executor_data={
                'command_line': ['echo', 'foo'],
            })

    TestHandler.executable_name = exe_name
    handler = TestHandler(expected_job_id=json.dumps(message.executor_data),
            expect_success=expect_success,
            default_environment={}, mandatory_environment={})

    handler._handle_message(message)

    reactor.run()



if __name__ == '__main__':
    args = parse_arguments()
    main(args.executable, args.expect_success)
