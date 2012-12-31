import logging
from rebase import Dictionary, Field, Model, Reference, Set
import time

LOG = logging.getLogger(__name__)

class Flow(Model):
    __namespace__ = 'flow'

    name = Field(String)
    status = Field(String)

    start_node = Field(Reference, contains=Field('StartNode'))
    stop_node = Field(Reference, contains=Field('StopNode'))

    @property
    def runtime(self):
        start = start_node.dispatch_time
        if start:
            stop = stop_node.complete_time
            if stop:
                return float(stop) - float(start)
            return time.time() - float(start)
