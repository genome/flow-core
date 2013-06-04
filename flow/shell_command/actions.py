from flow.petri_net.actions.base import BasicActionBase
from twisted.internet import defer

import logging


LOG = logging.getLogger(__name__)


class ShellCommandDispatchAction(BasicActionBase):
    net_constants = ['user_id', 'working_directory', 'mail_user']
    place_refs = []

    def _environment(self, net):
        return net.constant('environment')

    def _response_places(self):
        return {x: self.args[x] for x in self.place_refs}

    def _command_line(self, net, input_data_key):
        return self.args['command_line']

    def _executor_options(self, input_data_key, net):
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

        # Collect job-specific variables
        with_outputs = self.args.get('with_outputs')

        if input_data_key:
            executor_options['with_inputs'] = input_data_key

        if with_outputs:
            executor_options['with_outputs'] = with_outputs

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

    def execute(self, active_tokens_key, net, service_interfaces=None):
        token = None
        input_data_key = None
        token_color = self.active_color(active_tokens_key)

        input_data = self.input_data(active_tokens_key, net)

        LOG.debug('Inputs for %s: %r', self.name, input_data)

        if input_data:
            token = net.create_token(data=input_data,
                    data_type=self.output_token_type,
                    token_color=token_color)
            input_data_key = token.data.key
            LOG.debug("Created data token %s", token.key)

        executor_options = self._executor_options(input_data_key, net)
        cmdline = self._command_line(net, input_data_key)

        LOG.debug("Executor options: %r", executor_options)

        env = executor_options.get("environment", {})
        for k, v in env.iteritems():
            if k.startswith("FLOW"):
                LOG.debug("Flow environment variable set: %s=%s", k, v)

        response_places = self._response_places()
        deferred = service_interfaces[self.service_name].submit(
                command_line=cmdline,
                net_key=net.key,
                response_places=response_places,
                token_color=token_color,
                **executor_options
                )

        execute_deferred = defer.Deferred()
        deferred.addCallback(lambda _: execute_deferred.callback(token))
        return execute_deferred


class LSFDispatchAction(ShellCommandDispatchAction):
    service_name = "lsf"
    place_refs = ["post_dispatch_success",
            "post_dispatch_failure", "begin_execute",
            "execute_success", "execute_failure"]
    required_arguments = place_refs


class ForkDispatchAction(ShellCommandDispatchAction):
    service_name = "fork"
    place_refs = ["begin_execute", "execute_success",
            "execute_failure"]

    required_arguments = place_refs
