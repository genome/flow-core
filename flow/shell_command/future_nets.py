from flow.petri_net.future import FutureAction, FutureNet
from flow.shell_command.actions import ForkDispatchAction, LSFDispatchAction
from twisted.internet import defer

import logging


LOG = logging.getLogger(__name__)

class ShellCommandNet(FutureNet):
    def __init__(self, *args, **kwargs):
        FutureNet.__init__(self, *args, **kwargs)

        self.start = self.add_place('start')
        self.success = self.add_place('success')
        self.failure = self.add_place('failure')


class LSFCommandNet(ShellCommandNet):
    def __init__(self, name,
            dispatch_success_action=None,
            dispatch_failure_action=None,
            begin_execute_action=None,
            success_action=None,
            failure_action=None,
            **action_args):

        ShellCommandNet.__init__(self, name)

        self.dispatching = self.add_place("dispatching")
        self.pending = self.add_place("pending")
        self.running = self.add_place("running")
        self.dispatch_success_place = self.add_place("msg: dispatch_success")
        self.dispatch_failure_place = self.add_place("msg: dispatch_failure")
        self.begin_execute_place = self.add_place("msg: begin_execute")
        self.execute_success_place = self.add_place("msg: execute_success")
        self.execute_failure_place = self.add_place("msg: execute_failure")

        action_args.update({
                "post_dispatch_success": self.dispatch_success_place,
                "post_dispatch_failure": self.dispatch_failure_place,
                "begin_execute": self.begin_execute_place,
                "execute_success": self.execute_success_place,
                "execute_failure": self.execute_failure_place,
                })

        primary_action = FutureAction(LSFDispatchAction, **action_args)
        self.dispatch = self.add_basic_transition(name="dispatch",
                action=primary_action)

        self.dispatch_success = self.add_basic_transition("dispatch_success",
                action=dispatch_success_action)
        self.dispatch_failure = self.add_basic_transition("dispatch_failure",
                action=dispatch_failure_action)
        self.begin_execute = self.add_basic_transition("begin_execute",
                action=begin_execute_action)
        self.execute_success = self.add_basic_transition("execute_success",
                action=success_action)
        self.execute_failure = self.add_basic_transition("execute_failure",
                action=failure_action)

        self.start.add_arc_out(self.dispatch)
        self.dispatch.add_arc_out(self.dispatching)
        self.dispatching.add_arc_out(self.dispatch_success)
        self.dispatching.add_arc_out(self.dispatch_failure)
        self.dispatch_success_place.add_arc_out(self.dispatch_success)
        self.dispatch_failure_place.add_arc_out(self.dispatch_failure)

        self.dispatch_success.add_arc_out(self.pending)
        self.dispatch_failure.add_arc_out(self.failure)

        self.pending.add_arc_out(self.begin_execute)
        self.begin_execute_place.add_arc_out(self.begin_execute)
        self.begin_execute.add_arc_out(self.running)

        self.running.add_arc_out(self.execute_success)
        self.running.add_arc_out(self.execute_failure)
        self.execute_success_place.add_arc_out(self.execute_success)
        self.execute_failure_place.add_arc_out(self.execute_failure)

        self.execute_success.add_arc_out(self.success)
        self.execute_failure.add_arc_out(self.failure)


class ForkCommandNet(ShellCommandNet):
    def __init__(self, name,
            begin_execute_action=None,
            success_action=None,
            failure_action=None,
            **action_args):
        ShellCommandNet.__init__(self, name)

        self.dispatched = self.add_place("dispatched")
        self.running = self.add_place("running")

        self.on_begin_execute = self.add_place("msg: begin_execute")
        self.on_execute_success = self.add_place("msg: execute_success")
        self.on_execute_failure = self.add_place("msg: execute_failure")

        action_args.update({
                "begin_execute": self.on_begin_execute,
                "execute_success": self.on_execute_success,
                "execute_failure": self.on_execute_failure
                })

        primary_action = FutureAction(ForkDispatchAction, **action_args)
        self.dispatch = self.add_basic_transition(name="dispatch",
                action=primary_action)

        self.t_begin_execute = self.add_basic_transition("begin execute",
                action=begin_execute_action)
        self.execute_success = self.add_basic_transition("execute_success",
                action=success_action)
        self.execute_failure = self.add_basic_transition("execute_failure",
                action=failure_action)

        self.start.add_arc_out(self.dispatch)
        self.dispatch.add_arc_out(self.dispatched)

        self.dispatched.add_arc_out(self.t_begin_execute)
        self.on_begin_execute.add_arc_out(self.t_begin_execute)
        self.t_begin_execute.add_arc_out(self.running)

        self.running.add_arc_out(self.execute_success)
        self.running.add_arc_out(self.execute_failure)
        self.on_execute_success.add_arc_out(self.execute_success)
        self.on_execute_failure.add_arc_out(self.execute_failure)
        self.execute_success.add_arc_out(self.success)
        self.execute_failure.add_arc_out(self.failure)
