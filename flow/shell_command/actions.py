from flow.petri_net.actions.merge import BasicMergeAction

import logging


LOG = logging.getLogger(__name__)


class ShellCommandDispatchAction(BasicMergeAction):
    net_constants = ['user_id', 'working_directory', 'mail_user']
    place_refs = [
            "msg: dispatch_success",
            "msg: dispatch_failure",
            "msg: execute_begin",
            "msg: execute_success",
            "msg: execute_failure",
    ]
    required_arguments = place_refs + ['command_line']

    # Hooks that subclasses can override
    def command_line(self, token_data):
        return self.args['command_line']

    def inputs_hash_key(self, token_data):
        return


    # Private methods
    def _environment(self, net):
        return net.constant('environment')

    def _response_places(self):
        return {x: self.args[x] for x in self.place_refs}

    def _executor_options(self, net):
        executor_options = {}

        name = self.args.get('name', None)
        if name:
            executor_options['name'] = str(name)

        # Collect environment variables for this command
        environment = self._environment(net)
        if environment:
            executor_options['environment'] = environment

        # Collect other net-wide constants
        for opt in self.net_constants:
            value = net.constant(opt)
            if value:
                executor_options[opt] = value

        # Set logfiles
        stdout = self.args.get('stdout')
        if stdout:
            executor_options['stdout'] = stdout
        stderr = self.args.get('stderr')
        if stderr:
            executor_options['stderr'] = stderr

        # Collect resource requirements
        resources = self.args.get('resources', {})
        if resources:
            executor_options['resources'] = resources

        queue = self.args.get('queue')
        if queue:
            executor_options['queue'] = queue

        return executor_options

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        tokens, deferred = BasicMergeAction.execute(self, net,
                color_descriptor, active_tokens, service_interfaces)

        response_places = self._response_places()

        # BasicMergeAction returns exactly one token
        token_data = tokens[0].data

        command_line = self.command_line(token_data)
        inputs_hash_key = self.inputs_hash_key(token_data)
        executor_options = self._executor_options(net)
        callback_data = {
                'net_key': net.key,
                'color': color_descriptor.color,
                'color_group_idx': color_descriptor.group.idx,
        }
        callback_data.update(response_places)

        user_id = int(net.constant('user_id'))
        group_id = int(net.constant('group_id'))

        with_outputs = self.args.get('with_outputs')

        service = service_interfaces[self.service_name]
        deferred.addCallback(lambda x: service.submit(
            user_id=user_id, group_id=group_id,
            callback_data=callback_data, command_line=command_line,
            executor_data=executor_options))

        return tokens, deferred


class LSFDispatchAction(ShellCommandDispatchAction):
    service_name = "lsf"


class ForkDispatchAction(ShellCommandDispatchAction):
    service_name = "fork"
