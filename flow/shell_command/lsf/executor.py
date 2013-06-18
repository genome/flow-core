from flow import exit_codes
from flow.configuration.settings.injector import setting
from flow.shell_command import factory
from flow.shell_command.executor_base import ExecutorBase, send_message
from flow.shell_command.lsf import options
from flow.shell_command.lsf import resource
from injector import inject
from pythonlsf import lsf
from twisted.python.procutils import which

import logging
import os


LOG = logging.getLogger(__name__)


@inject(pre_exec=setting('shell_command.lsf.pre_exec'),
        post_exec=setting('shell_command.lsf.post_exec'),
        resource_definitions=setting('shell_command.lsf.available_resources'),
        option_definitions=setting('shell_command.lsf.available_options'),
        default_options=setting('shell_command.lsf.default_options'))
class LSFExecutor(ExecutorBase):
    def __init__(self):
        if self.pre_exec:
            self.pre_exec_command = [_find_executable(self.pre_exec[0])] +\
                self.pre_exec[1:]
        else:
            self.pre_exec_command = None

        if self.post_exec:
            self.post_exec_command = [_find_executable(self.post_exec[0])] +\
                self.post_exec[1:]
        else:
            self.post_exec_command = None

        self.available_options = factory.build_objects(
                self.option_definitions, options, 'LSFOption')

        self.available_resources = {}
        self.available_resources['limit'] = factory.build_objects(
                self.resource_definitions.get('limit', {}), resource)
        self.available_resources['request'] = factory.build_objects(
                self.resource_definitions.get('request', {}), resource)
        self.available_resources['reserve'] = factory.build_objects(
                self.resource_definitions.get('reserve', {}), resource)


    def on_job_id(self, job_id, callback_data, service_interfaces):
        data = {'job_id': job_id}
        return send_message('msg: dispatch_success', callback_data,
                service_interfaces, token_data=data)

    def on_failure(self, exit_code, callback_data, service_interfaces):
        data = {'exit_code': exit_code}
        return send_message('msg: dispatch_failure', callback_data,
                service_interfaces, token_data=data)


    def execute_command_line(self, job_id_callback, command_line,
            executor_data, resources):
        request = self.construct_request(command_line, executor_data, resources)

        reply = create_reply()

        try:
            submit_result = lsf.lsb_submit(request, reply)
        except:
            LOG.exception("lsb_submit failed for command line: %s "
                    "-- with executor_data: %s -- with resources: %s",
                    command_line, executor_data, resources)
            raise

        if submit_result > 0:
            job_id_callback(submit_result)
            LOG.debug('successfully submitted lsf job: %s', submit_result)
            return exit_codes.EXECUTE_SUCCESS

        else:
            lsf.lsb_perror("lsb_submit")
            LOG.error('failed to submit lsf job, return value = (%s), err = %s',
                    submit_result, lsf.lsb_sperror("lsb_submit"))
            return exit_codes.EXECUTE_FAILURE


    def construct_request(self, command_line, executor_data, resources):
        request = create_empty_request()

        if self.post_exec is not None:
            self.set_post_exec(request, executor_data)

        if self.pre_exec is not None:
            self.set_pre_exec(request, executor_data)

        resource.set_all_resources(request, resources, self.available_resources)
        self.set_options(request, executor_data)

        request.command = ' '.join(command_line)

        return request


    def set_pre_exec(self, request, executor_data):
        response_places = {
                'msg: execute_begin': '--execute-begin',
        }
        pre_exec_command = make_pre_post_command_string(self.pre_exec_command,
                executor_data, response_places)

        request.preExecCmd = str(pre_exec_command)
        request.options |= lsf.SUB_PRE_EXEC

    def set_post_exec(self, request, executor_data):
        response_places = {
                'msg: execute_success': '--execute-success',
                'msg: execute_failure': '--execute-failure',
        }
        post_exec_command = make_pre_post_command_string(self.post_exec_command,
                executor_data, response_places)

        request.postExecCmd = str(post_exec_command)
        request.options3 |= lsf.SUB3_POST_EXEC


    def set_options(self, request, executor_data):
        for option, value in self.default_options.iteritems():
            self.available_options[option].set_option(request, value)

        lsf_options = executor_data.get('lsf_options', {})

        for name, value in lsf_options.iteritems():
            self.available_options[name].set_option(request, value)


def create_empty_request():
    request = lsf.submit()
    request.options = 0
    request.options2 = 0
    request.options3 = 0

    return request


def create_reply():
    reply = lsf.submitReply()

    init_code = lsf.lsb_init('')
    if init_code > 0:
        raise RuntimeError("Failed lsb_init, errno = %d" % lsf.lsb_errno())

    return reply


def make_pre_post_command_string(executable, executor_data, response_places):
    base_arguments = ("--net-key '%(net_key)s' --color %(color)s "
            "--color-group-idx %(color_group_idx)s" % executor_data)

    base_command_line = ["'%s'" % executable, base_arguments]

    for place_name, command_line_flag in response_places.iteritems():
        base_command_line.extend([
            command_line_flag,
            "%s" % str(executor_data[place_name]),
        ])

    lsf_options = executor_data.get('lsf_options', {})
    if 'stdout' in lsf_options:
        base_command_line.append("1>> '%s'" % lsf_options['stdout'])

    if 'stderr' in lsf_options:
        base_command_line.append("2>> '%s'" % lsf_options['stderr'])

    return 'bash -c "%s"' % ' '.join(base_command_line)


def _find_executable(name):
    executables = which(name)
    if executables:
        return executables[0]
    else:
        raise RuntimeError("Couldn't find the executable (%s) in PATH: %s"
                % (name, os.environ.get('PATH', None)))
