import flow.petri.netbuilder as nb
import flow.petri.safenet as sn

import os

class CommandLineDispatchAction(sn.TransitionAction):
    net_variables = ["environment", "user_id", "working_directory", "mail_user"]

    def _response_places(self):
        raise NotImplementedError(
            "In class %s: _response_place not implemented" %
            self.__class__.__name__)

    def _command_line(self, net, input_data_key):
        return self.args["command_line"]

    def _executor_options(self, input_data, net):
        # Collect net-wide variables
        executor_options = {}
        for opt in self.net_variables:
            value = net.constant(opt)
            if value:
                executor_options[opt] = value

        # Collect job-specific variables
        with_outputs = self.args.get("with_outputs")

        if input_data and len(input_data):
            executor_options["with_inputs"] = input_data.key

        if with_outputs:
            executor_options["with_outputs"] = with_outputs

        return executor_options


    def execute(self, input_data, net, services=None):
        executor_options = self._executor_options(input_data, net)

        response_places = self._response_places()
        services[self.service_name].submit(
                command_line=self._command_line(net, input_data.key),
                net_key=net.key,
                response_places=response_places,
                **executor_options
                )


class LSFDispatchAction(CommandLineDispatchAction):
    service_name = "lsf"

    dispatch_success = 0
    dispatch_failure = 1
    begin_execute = 2
    execute_success = 3
    execute_failure = 4

    def _response_places(self):
        return {
            'post_dispatch_success': self.place_refs[self.dispatch_success],
            'post_dispatch_failure': self.place_refs[self.dispatch_failure],
            'begin_execute': self.place_refs[self.begin_execute],
            'execute_success': self.place_refs[self.execute_success],
            'execute_failure': self.place_refs[self.execute_failure],
        }


class LocalDispatchAction(CommandLineDispatchAction):
    service_name = "fork"

    begin_execute = 0
    execute_success = 1
    execute_failure = 2

    def _response_places(self):
        return {
            'begin_execute': self.place_refs[self.begin_execute],
            'execute_success': self.place_refs[self.execute_success],
            'execute_failure': self.place_refs[self.execute_failure],
        }


class LSFCommandNet(nb.SuccessFailureNet):
    def __init__(self, builder, name, action_class, action_args={}):
        nb.SuccessFailureNet.__init__(self, builder, name)

        self.dispatching = self.add_place("dispatching")
        self.pending = self.add_place("pending")
        self.running = self.add_place("running")
        self.dispatch_success_place = self.add_place("msg: dispatch_success")
        self.dispatch_failure_place = self.add_place("msg: dispatch_failure")
        self.begin_execute_place = self.add_place("msg: begin_execute")
        self.execute_success_place = self.add_place("msg: execute_success")
        self.execute_failure_place = self.add_place("msg: execute_failure")

        self.dispatch = self.add_transition(
                name="dispatch",
                action_class=action_class,
                action_args=action_args,
                place_refs=[
                    self.dispatch_success_place.index,
                    self.dispatch_failure_place.index,
                    self.begin_execute_place.index,
                    self.execute_success_place.index,
                    self.execute_failure_place.index,
                    ]
                )

        self.dispatch_success = self.add_transition("dispatch_success")
        self.dispatch_failure = self.add_transition("dispatch_failure")
        self.begin_execute = self.add_transition("begin_execute")
        self.execute_success = self.add_transition("execute_success")
        self.execute_failure = self.add_transition("execute_failure")

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


class LocalCommandNet(nb.SuccessFailureNet):
    def __init__(self, builder, name, action_class, action_args={}):
        nb.SuccessFailureNet.__init__(self, builder, name)

        self.dispatched = self.add_place("dispatched")
        self.running = self.add_place("running")

        self.on_begin_execute = self.add_place("msg: begin_execute")
        self.on_execute_success = self.add_place("msg: execute_success")
        self.on_execute_failure = self.add_place("msg: execute_failure")

        self.dispatch = self.add_transition(
                name="dispatch",
                action_class=action_class,
                action_args=action_args,
                place_refs=[
                    self.on_begin_execute.index,
                    self.on_execute_success.index,
                    self.on_execute_failure.index
                ],
            )

        self.t_begin_execute = self.add_transition("begin execute")
        self.execute_success = self.add_transition("execute_success")
        self.execute_failure = self.add_transition("execute_failure")

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
