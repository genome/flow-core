from flow.petri_net.actions.merge import BasicMergeAction

import logging


LOG = logging.getLogger(__name__)


class ShellCommandDispatchAction(BasicMergeAction):
    net_constants = ['group_id', 'user_id', 'working_directory']
    place_refs = [
            "msg: dispatch_failure",
            "msg: dispatch_success",
            "msg: execute_begin",
            "msg: execute_failure",
            "msg: execute_success",
    ]
    required_arguments = place_refs + ['command_line']

    # Hooks that subclasses can override
    def command_line(self, token_data):
        return self.args['command_line']


    # Private methods
    def _response_places(self):
        return {x: self.args[x] for x in self.place_refs}

    def _executor_data(self, net):
        executor_data = {}

        self._set_environment(net, executor_data)
        self._set_constants(net, executor_data)
        self._set_io_files(executor_data)

        if 'resources' in self.args:
            executor_data['resources'] = self.args['resources']

        if 'lsf_options' in self.args:
            executor_data['lsf_options'] = self.args['lsf_options']

        return executor_data

    def _set_environment(self, net, executor_data):
        environment = net.constant('environment')
        if environment:
            executor_data['environment'] = environment

    def _set_constants(self, net, executor_data):
        for opt in self.net_constants:
            value = net.constant(opt)
            if value:
                executor_data[opt] = value

    def _set_io_files(self, executor_data):
        if 'stderr' in self.args:
            executor_data['stderr'] = self.args['stderr']
        if 'stdin' in self.args:
            executor_data['stdin'] = self.args['stdin']
        if 'stdout' in self.args:
            executor_data['stdout'] = self.args['stdout']


    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        tokens, deferred = BasicMergeAction.execute(self, net,
                color_descriptor, active_tokens, service_interfaces)

        response_places = self._response_places()

        # BasicMergeAction returns exactly one token
        token_data = tokens[0].data

        command_line = self.command_line(token_data)
        executor_data = self._executor_data(net)
        callback_data = {
                'net_key': net.key,
                'color': color_descriptor.color,
                'color_group_idx': color_descriptor.group.idx,
        }
        callback_data.update(response_places)

        user_id = int(net.constant('user_id'))
        group_id = int(net.constant('group_id'))
        working_directory = net.constant('working_directory', '/tmp')

        service = service_interfaces[self.service_name]
        deferred.addCallback(lambda x: service.submit(user_id=user_id,
            group_id=group_id, working_directory=working_directory,
            callback_data=callback_data, command_line=command_line,
            executor_data=executor_data))

        return tokens, deferred


class LSFDispatchAction(ShellCommandDispatchAction):
    service_name = "lsf"


class ForkDispatchAction(ShellCommandDispatchAction):
    service_name = "fork"
