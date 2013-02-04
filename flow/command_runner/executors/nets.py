import flow.petri.netbuilder as nb
import flow.petri.safenet as sn

class LSFDispatchAction(sn.TransitionAction):
    dispatch_success = 0
    dispatch_failure = 1
    begin_execute = 2
    execute_success = 3
    execute_failure = 4

    def _response_places(self):
        return {
            'dispatch_success_place': self.place_refs[self.dispatch_success],
            'dispatch_failure_place': self.place_refs[self.dispatch_failure],
            'begin_execute_place': self.place_refs[self.begin_execute],
            'execute_success_place': self.place_refs[self.execute_success],
            'execute_failure_place': self.place_refs[self.execute_failure],
        }

    def execute(self, net, services=None):
        #services["genome_execute"].submit(
                #command_line=self.args,
                #net_key=net.key,
                #response_places=self._response_places()
                #)
        print "Execute %r" % self.args.value


class LSFCommandNet(nb.SuccessFailureNet):
    def __init__(self, builder, name, cmdline):
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
                action_class=LSFDispatchAction,
                action_args=cmdline,
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
