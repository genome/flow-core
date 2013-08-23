import flow.rabbit.reporter.base
import pprint


class PrettyReporter(flow.rabbit.reporter.base.IReporter):
    def __call__(self, result):
        pprint.pprint(result)
