from flow.petri_net.actions.remove_data import RemoveDataAction
from flow.petri_net.future import FutureAction
from flow.petri_net.success_failure_net import SuccessFailureNet
from flow.shell_command.petri_net import actions

import logging


LOG = logging.getLogger(__name__)


class ShellCommandNet(SuccessFailureNet):
    def __init__(self, name, dispatch_action_class, **action_args):
        SuccessFailureNet.__init__(self, name)

        # state-related places
        self.dispatching_place = self.add_place("dispatching")
        self.pending_place = self.add_place("pending")
        self.running_place = self.add_place("running")

        # messaging places
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

        primary_action = FutureAction(dispatch_action_class, **action_args)

        self.dispatch_transition = self.add_basic_transition(
                name="dispatch", action=primary_action)

        # messaging transitions
        self.dispatch_success_transition = self.add_basic_transition(
                "dispatch_success")
        self.dispatch_failure_transition = self.add_basic_transition(
                "dispatch_failure")
        self.execute_begin_transition = self.add_basic_transition(
                "execute_begin")

        self.execute_success_transition = self.add_basic_transition(
                "execute_success")
        self.execute_failure_transition = self.add_basic_transition(
                "execute_failure")

        # remove spurious token data
        self.token_cleanup_action = FutureAction(RemoveDataAction,
                fields=['hostname', 'job_id', 'exit_code'])
        self.success_cleanup_transition = self.add_basic_transition(
                'success_cleanup', action=self.token_cleanup_action)
        self.failure_cleanup_transition = self.add_basic_transition(
                'failure_cleanup', action=self.token_cleanup_action)

        # connecting things together
        self.internal_start_place = self.bridge_transitions(
                self.internal_start_transition, self.dispatch_transition,
                name='start_bridge')
        self.dispatch_transition.add_arc_out(self.dispatching_place)
        self.dispatching_place.add_arc_out(self.dispatch_success_transition)
        self.dispatching_place.add_arc_out(self.dispatch_failure_transition)
        self.dispatch_success_place.add_arc_out(
                self.dispatch_success_transition)
        self.dispatch_failure_place.add_arc_out(
                self.dispatch_failure_transition)

        self.dispatch_success_transition.add_arc_out(self.pending_place)

        self.pending_place.add_arc_out(self.execute_begin_transition)
        self.execute_begin_place.add_arc_out(self.execute_begin_transition)
        self.execute_begin_transition.add_arc_out(self.running_place)

        self.running_place.add_arc_out(self.execute_success_transition)
        self.running_place.add_arc_out(self.execute_failure_transition)
        self.execute_success_place.add_arc_out(self.execute_success_transition)
        self.execute_failure_place.add_arc_out(self.execute_failure_transition)


        self.success_cleanup_place = self.bridge_transitions(
                self.execute_success_transition,
                self.success_cleanup_transition, name='success_cleanup')
        self.failure_cleanup_place = self.join_transitions_as_or(
                destination=self.failure_cleanup_transition,
                sources=[self.execute_failure_transition,
                         self.dispatch_failure_transition])

        self.internal_success_place = self.bridge_transitions(
                self.success_cleanup_transition,
                self.internal_success_transition, 'success_bridge')
        self.internal_failure_place = self.bridge_transitions(
                self.failure_cleanup_transition,
                self.internal_failure_transition, 'failure_bridge')
