from flow.petri_net import future


class SuccessFailureNet(future.FutureNet):
    def __init__(self, name=''):
        future.FutureNet.__init__(self, name=name)

        # External input/output places -- owner nets should connect to these
        self.start_place = self.add_place('start')

        self.done_place = self.add_place('done')
        self.success_place = self.add_place('success')
        self.failure_place = self.add_place('failure')


        # Internal input/output places -- subclasses should connect to these
        self.internal_start_place = self.add_place('internal-start')

        self.internal_failure_place = self.add_place('internal-failure')
        self.internal_success_place = self.add_place('internal-success')


        # Private logic to make "done" seamless
        self._internal_done_place = self.add_place()
        self._internal_failure_place = self.add_place()
        self._internal_success_place = self.add_place()

        self.split_place(self.internal_failure_place,
                [self._internal_done_place, self._internal_failure_place])
        self.split_place(self.internal_success_place,
                [self._internal_done_place, self._internal_success_place])


        # Transitions to observe -- owners and subclasses may observe these
        self.start_transition = self.bridge_places(
                self.start_place, self.internal_start_place,
                name='start')

        self.done_transition = self.bridge_places(
                self._internal_done_place, self.done_place,
                name='done')
        self.failure_transition = self.bridge_places(
                self._internal_failure_place, self.failure_place,
                name='failure')
        self.success_transition = self.bridge_places(
                self._internal_success_place, self.success_place,
                name='success')
