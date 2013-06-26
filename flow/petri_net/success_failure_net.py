from flow.petri_net.future import FutureNet


class SuccessFailureNet(FutureNet):
    def __init__(self, name=''):
        FutureNet.__init__(self, name=name)

        # External -- owner nets should connect to these
        self.start_transition = self.add_basic_transition(name='start')
        self.success_transition = self.add_basic_transition(name='success')
        self.failure_transition = self.add_basic_transition(name='failure')

        # Internal -- subclasses should connect to these
        self.internal_start_transition = self.start_transition
        self.internal_success_transition = self.success_transition
        self.internal_failure_transition = self.failure_transition

    def wrap_with_places(self):
        """
        Attach places to the start, success, and failure transitions.
        """
        self.start_place = self.add_place(name='start')
        self.start_place.add_arc_out(self.start_transition)

        self.success_place = self.add_place(name='success')
        self.success_place.add_arc_in(self.success_transition)

        self.failure_place = self.add_place(name='failure')
        self.failure_place.add_arc_in(self.failure_transition)
