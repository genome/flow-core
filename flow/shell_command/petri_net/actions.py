from flow.petri_net.actions.merge import BasicMergeAction

import logging


LOG = logging.getLogger(__name__)


class ShellCommandDispatchAction(BasicMergeAction):
    place_refs = [
            "msg: dispatch_failure",
            "msg: dispatch_success",
            "msg: execute_begin",
            "msg: execute_failure",
            "msg: execute_success",
    ]
    required_arguments = place_refs

    # Hooks that subclasses can override
    def command_line(self, net, token_data):
        return self.args['command_line']


    # Private methods
    def _response_places(self):
        return {x: self.args[x] for x in self.place_refs}

    def executor_data(self, net, color_descriptor, token_data, response_places):
        executor_data = {}

        executor_data['command_line'] = self.command_line(net, token_data)
        umask = net.constant('umask')
        if umask:
            executor_data['umask'] = int(umask)

        self.set_io_files(net, executor_data, token_data)

        return executor_data

    def callback_data(self, net, color_descriptor, response_places):
        result = {
                u'net_key': net.key,
                u'color': color_descriptor.color,
                u'color_group_idx': color_descriptor.group.idx,
        }
        result.update(response_places)
        return result

    def set_io_files(self, net, executor_data, token_data):
        if 'stderr' in self.args:
            executor_data['stderr'] = self.args['stderr']
        if 'stdin' in self.args:
            executor_data['stdin'] = self.args['stdin']
        if 'stdout' in self.args:
            executor_data['stdout'] = self.args['stdout']

    def base_message_params(self, net, color_descriptor):
        params = {
            'user_id': int(net.constant('user_id')),
            'group_id': int(net.constant('group_id')),
            'working_directory': net.constant('working_directory', '/tmp'),
        }

        params['environment'] = self.environment(net, color_descriptor)

        return params

    def environment(self, net, color_descriptor):
        return net.constant('environment', {})


    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        tokens, deferred = BasicMergeAction.execute(self, net,
                color_descriptor, active_tokens, service_interfaces)

        response_places = self._response_places()

        # BasicMergeAction returns exactly one token
        token_data = tokens[0].data

        service = service_interfaces[self.service_name]
        deferred.addCallback(lambda x: service.submit(
            callback_data=self.callback_data(net,
                color_descriptor, response_places),
            executor_data=self.executor_data(net, color_descriptor,
                token_data, response_places),
            **self.base_message_params(net, color_descriptor)))

        return tokens, deferred


class LSFDispatchAction(ShellCommandDispatchAction):
    service_name = "lsf"

    def executor_data(self, net, color_descriptor, token_data, response_places):
        executor_data = ShellCommandDispatchAction.executor_data(self, net,
            color_descriptor, token_data, response_places)

        executor_data['resources'] = self.args.get('resources', {})
        if 'lsf_options' in self.args:
            executor_data['lsf_options'] = self.args['lsf_options']

        executor_data.update(self.callback_data(net,
            color_descriptor, response_places))

        return executor_data


class ForkDispatchAction(ShellCommandDispatchAction):
    service_name = "fork"
