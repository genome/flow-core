from flow import petri

import flow.petri.netbuilder as nb
import logging
import os


LOG = logging.getLogger(__name__)


class ShellCommandDispatchAction(petri.TransitionAction):
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
            token = petri.Token.create(self.connection, data=input_data,
                    data_type=self.output_token_type)
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
        service_interfaces[self.service_name].submit(
                command_line=cmdline,
                net_key=net.key,
                response_places=response_places,
                token_color=token_color,
                **executor_options
                )

        return token


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


class LSFCommandNet(nb.SuccessFailureNet):
    def __init__(self, builder, name, action_class, action_args={},
            dispatch_success_action=None,
            dispatch_failure_action=None,
            begin_execute_action=None,
            success_action=None,
            failure_action=None):

        nb.SuccessFailureNet.__init__(self, builder, name)

        self.dispatching = self.add_place("dispatching")
        self.pending = self.add_place("pending")
        self.running = self.add_place("running")
        self.dispatch_success_place = self.add_place("msg: dispatch_success")
        self.dispatch_failure_place = self.add_place("msg: dispatch_failure")
        self.begin_execute_place = self.add_place("msg: begin_execute")
        self.execute_success_place = self.add_place("msg: execute_success")
        self.execute_failure_place = self.add_place("msg: execute_failure")

        args = dict(action_args)
        args.update({
                "post_dispatch_success": self.dispatch_success_place.index,
                "post_dispatch_failure": self.dispatch_failure_place.index,
                "begin_execute": self.begin_execute_place.index,
                "execute_success": self.execute_success_place.index,
                "execute_failure": self.execute_failure_place.index,
                })

        dispatch_action = nb.ActionSpec(cls=action_class, args=args)

        self.dispatch = self.add_transition(name="dispatch",
                action=dispatch_action)

        self.dispatch_success = self.add_transition("dispatch_success",
                action=dispatch_success_action)
        self.dispatch_failure = self.add_transition("dispatch_failure",
                action=dispatch_failure_action)
        self.begin_execute = self.add_transition("begin_execute",
                action=begin_execute_action)
        self.execute_success = self.add_transition("execute_success",
                action=success_action)
        self.execute_failure = self.add_transition("execute_failure",
                action=failure_action)

        self.start.arcs_out.add(self.dispatch)
        self.dispatch.arcs_out.add(self.dispatching)
        self.dispatching.arcs_out.add(self.dispatch_success)
        self.dispatching.arcs_out.add(self.dispatch_failure)
        self.dispatch_success_place.arcs_out.add(self.dispatch_success)
        self.dispatch_failure_place.arcs_out.add(self.dispatch_failure)

        self.dispatch_success.arcs_out.add(self.pending)
        self.dispatch_failure.arcs_out.add(self.failure)

        self.pending.arcs_out.add(self.begin_execute)
        self.begin_execute_place.arcs_out.add(self.begin_execute)
        self.begin_execute.arcs_out.add(self.running)

        self.running.arcs_out.add(self.execute_success)
        self.running.arcs_out.add(self.execute_failure)
        self.execute_success_place.arcs_out.add(self.execute_success)
        self.execute_failure_place.arcs_out.add(self.execute_failure)

        self.execute_success.arcs_out.add(self.success)
        self.execute_failure.arcs_out.add(self.failure)


class ForkCommandNet(nb.SuccessFailureNet):
    def __init__(self, builder, name, action_class, action_args={},
            begin_execute_action=None,
            success_action=None,
            failure_action=None):

        nb.SuccessFailureNet.__init__(self, builder, name)

        self.dispatched = self.add_place("dispatched")
        self.running = self.add_place("running")

        self.on_begin_execute = self.add_place("msg: begin_execute")
        self.on_execute_success = self.add_place("msg: execute_success")
        self.on_execute_failure = self.add_place("msg: execute_failure")

        args = dict(action_args)
        args.update({
                "begin_execute": self.on_begin_execute.index,
                "execute_success": self.on_execute_success.index,
                "execute_failure": self.on_execute_failure.index
                })

        dispatch_action = nb.ActionSpec(cls=action_class, args=args)

        self.dispatch = self.add_transition(name="dispatch",
                action=dispatch_action)

        self.t_begin_execute = self.add_transition("begin execute",
                action=begin_execute_action)
        self.execute_success = self.add_transition("execute_success",
                action=success_action)
        self.execute_failure = self.add_transition("execute_failure",
                action=failure_action)

        self.start.arcs_out.add(self.dispatch)
        self.dispatch.arcs_out.add(self.dispatched)

        self.dispatched.arcs_out.add(self.t_begin_execute)
        self.on_begin_execute.arcs_out.add(self.t_begin_execute)
        self.t_begin_execute.arcs_out.add(self.running)

        self.running.arcs_out.add(self.execute_success)
        self.running.arcs_out.add(self.execute_failure)
        self.on_execute_success.arcs_out.add(self.execute_success)
        self.on_execute_failure.arcs_out.add(self.execute_failure)
        self.execute_success.arcs_out.add(self.success)
        self.execute_failure.arcs_out.add(self.failure)
