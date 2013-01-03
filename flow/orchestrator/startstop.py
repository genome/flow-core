import logging

LOG = logging.getLogger(__name__)

class StartStopNode(Job):
    def start(self, **kwargs):
        Job.start(self, **kwargs)
        self.complete()

class StartNode(StartStopNode): pass
# XXX Do we need something like this?
#    def start(self, **kwargs):
#        StartStopNode.start(self, **kwargs)
#        self.flow.start(**kwargs)


class StopNode(StartStopNode): pass
# XXX Do we need something like this?
#    def complete(self):
#        StartStopNode.complete(self)
#        self.flow.complete()
