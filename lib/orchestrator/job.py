import logging
from rebase import Dictionary, Field, Integer, Model, Reference, Set, String

LOG = logging.getLogger(__name__)

class Job(Model):
    __namespace__ = 'job'

    flow = Field(Reference, contains=Field('Flow'))

    in_degree = Field(Integer)
    successors = Field(Set, contains=Field('Job'))

    # Properties
    name = Field(String)
    dispatch_time = Field(String)
    complete_time = Field(String)
    status = Field(String)


    def complete(self):
        self.complete_time = time.time()
#        self.status = 'Successful'
        ready_jobs = []
        for s in self.successors:
            s.in_degree.increment(by=-1)
            if in_degree == 0:
                ready_jobs.append(s)

        return ready_jobs

    def fail(self):
        self.complete_time = time.time()
        # Set status to 'failed'?

    def start(self, **kwargs):
        self.dispatch_time = time.time()
        # Set status to 'scheduled'?


class MessageJob(Job):
    # Params are for things that the engine needs to use to launch the job
    parameters = Field(Dictionary)

    routing_key = Field(String)

    def start(self, publish_function=None, **kwargs):
        Job.start(self, **kwargs)
