import flow.petri.netbuilder as nb
import flow.petri.safenet as sn

class LSFDispatch(sn.TransitionAction):
    def execute(self, net, services=None):
        return_identifier = {}
        services["genome_execute"].submit(
                command_line=self.args,
                return_identifier=return_identifier,
                **args
                )
        print "Execute %r" % self.args.value


class LSFCommandNet(nb.SuccessFailureNet):
    def __init__(self, builder, name, cmdline):
        nb.SuccessFailureNet.__init__(self, builder, name)

        self.dispatching = self.add_place("dispatching")
        self.pending = self.add_place("pending")
        self.running = self.add_place("running")
        self.dispatch_success_place = self.add_place("msg: dispatch_success")
        self.dispatch_failure_place = self.add_place("msg: dispatch_failure")
        self.execute_begin_place = self.add_place("msg: execute_begin")
        self.execute_success_place = self.add_place("msg: execute_success")
        self.execute_failure_place = self.add_place("msg: execute_failure")


        self.dispatch = self.add_transition(
                name="dispatch",
                action_class=LSFDispatch,
                action_args=cmdline,
                place_refs=[
                    self.dispatch_success_place,
                    self.dispatch_failure_place,
                    self.execute_success_place,
                    self.execute_failure_place,
                    ]
                )
        self.dispatch_success = self.add_transition("dispatch_success")
        self.dispatch_failure = self.add_transition("dispatch_failure")
        self.execute_begin = self.add_transition("execute_begin")
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

        self.pending.arcs_out.add(self.execute_begin)
        self.execute_begin_place.arcs_out.add(self.execute_begin)
        self.execute_begin.arcs_out.add(self.running)

        self.running.arcs_out.add(self.execute_success)
        self.running.arcs_out.add(self.execute_failure)
        self.execute_success_place.arcs_out.add(self.execute_success)
        self.execute_failure_place.arcs_out.add(self.execute_failure)

        self.execute_success.arcs_out.add(self.success)
        self.execute_failure.arcs_out.add(self.failure)

if __name__ == "__main__":
    builder = nb.NetBuilder('test')
    net = LSFCommandNet(builder, "test", ["ls", "-al"])
    builder.graph().draw("x.ps", prog="dot")
