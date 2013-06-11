from flow.petri_net.future import FutureAction, FutureNet
from flow.shell_command.actions import ForkDispatchAction, LSFDispatchAction

import logging


LOG = logging.getLogger(__name__)


class ShellCommandNet(FutureNet):
    def __init__(self, name,
            dispatch_success_action=None,
            dispatch_failure_action=None,
            execute_begin_action=None,
            success_action=None,
            failure_action=None,
            **action_args):

        FutureNet.__init__(self, name)

        self.start = self.add_place('start')
        self.success = self.add_place('success')
        self.failure = self.add_place('failure')

        self.dispatching = self.add_place("dispatching")
        self.pending = self.add_place("pending")
        self.running = self.add_place("running")

        self.dispatch_success_place = self.add_place("msg: dispatch_success")
        self.dispatch_failure_place = self.add_place("msg: dispatch_failure")
        self.execute_begin_place = self.add_place("msg: execute_begin")
        self.execute_success_place = self.add_place("msg: execute_success")
        self.execute_failure_place = self.add_place("msg: execute_failure")

        action_args.update({
            "msg: dispatch_success": self.dispatch_success_place,
            "msg: dispatch_failure": self.dispatch_failure_place,
            "msg: execute_begin": self.execute_begin_place,
            "msg: execute_success": self.execute_success_place,
            "msg: execute_failure": self.execute_failure_place,
        })

        primary_action = FutureAction(self.DISPATCH_ACTION, **action_args)

        self.dispatch = self.add_basic_transition(name="dispatch",
                action=primary_action)

        self.dispatch_success = self.add_basic_transition("dispatch_success",
                action=dispatch_success_action)
        self.dispatch_failure = self.add_basic_transition("dispatch_failure",
                action=dispatch_failure_action)
        self.execute_begin = self.add_basic_transition("execute_begin",
                action=execute_begin_action)
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

        self.pending.add_arc_out(self.execute_begin)
        self.execute_begin_place.add_arc_out(self.execute_begin)
        self.execute_begin.add_arc_out(self.running)

        self.running.add_arc_out(self.execute_success)
        self.running.add_arc_out(self.execute_failure)
        self.execute_success_place.add_arc_out(self.execute_success)
        self.execute_failure_place.add_arc_out(self.execute_failure)

        self.execute_success.add_arc_out(self.success)
        self.execute_failure.add_arc_out(self.failure)


class LSFCommandNet(ShellCommandNet):
    DISPATCH_ACTION = LSFDispatchAction


class ForkCommandNet(ShellCommandNet):
    DISPATCH_ACTION = ForkDispatchAction
