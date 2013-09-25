from flow.configuration.settings.injector import setting
from flow.shell_command.lsf.options import LSFOptionManager
from flow.shell_command.lsf.resource_manager import LSFResourceManager
from pythonlsf import lsf
from twisted.python.procutils import which

import injector
import logging
import os


LOG = logging.getLogger(__name__)


@injector.inject(
    resource_manager=LSFResourceManager,
    option_manager=LSFOptionManager,
    pre_exec=setting('shell_command.lsf.pre_exec'),
    post_exec=setting('shell_command.lsf.post_exec'))
class LSFRequestBuilder(object):
    def __init__(self):
        if self.pre_exec:
            self.pre_exec_command = _localize_cmd(self.pre_exec)
        else:
            self.pre_exec_command = None

        if self.post_exec:
            self.post_exec_command = _localize_cmd(self.post_exec)
        else:
            self.post_exec_command = None

    def construct_request(self, executor_data):
        request = create_empty_request()

        if self.post_exec is not None:
            self.set_post_exec(request, executor_data)

        if self.pre_exec is not None:
            self.set_pre_exec(request, executor_data)

        self.resource_manager.set_resources(request, executor_data)
        self.option_manager.set_options(request, executor_data)

        request.command = str(' '.join("'%s'" % word
            for word in executor_data['command_line']))

        self.set_pre_exec(request, executor_data)
        self.set_post_exec(request, executor_data)

        return request

    def set_pre_exec(self, request, executor_data):
        response_places = {
                'msg: execute_begin': '--execute-begin',
        }
        pre_exec_command = make_pre_post_command_string(self.pre_exec_command,
                executor_data, response_places)
        LOG.debug('pre-exec command: %s', pre_exec_command)

        request.preExecCmd = str(pre_exec_command)
        request.options |= lsf.SUB_PRE_EXEC

    def set_post_exec(self, request, executor_data):
        response_places = {
                'msg: execute_success': '--execute-success',
                'msg: execute_failure': '--execute-failure',
        }
        post_exec_command = make_pre_post_command_string(self.post_exec_command,
                executor_data, response_places)
        LOG.debug('post-exec command: %s', post_exec_command)

        request.postExecCmd = str(post_exec_command)
        request.options3 |= lsf.SUB3_POST_EXEC


def create_empty_request():
    request = lsf.submit()
    request.options = 0
    request.options2 = 0
    request.options3 = 0

    return request


def make_pre_post_command_string(executable, executor_data, response_places):
    base_arguments = ("--net-key '%(net_key)s' --color %(color)s "
            "--color-group-idx %(color_group_idx)s" % executor_data)

    base_command_line = executable + [base_arguments]

    for place_name, command_line_flag in response_places.iteritems():
        base_command_line.extend([
            command_line_flag,
            "%s" % str(executor_data[place_name]),
        ])

    if 'stdout' in executor_data:
        base_command_line.append("1>> '%s'" % executor_data['stdout'])

    if 'stderr' in executor_data:
        base_command_line.append("2>> '%s'" % executor_data['stderr'])

    return 'bash -c "%s"' % ' '.join(base_command_line)


def _localize_cmd(cmd):
    command = cmd[0]
    localized_command = _find_executable(command)
    return ["'%s'" % localized_command] + cmd[1:]


def _find_executable(name):
    executables = which(name)
    if executables:
        return executables[0]
    else:
        raise RuntimeError("Couldn't find the executable (%s) in PATH: %s"
                % (name, os.environ.get('PATH', None)))
