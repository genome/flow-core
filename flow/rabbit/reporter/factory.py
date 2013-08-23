from flow.rabbit.reporter import pretty


class ReporterFactory(object):
    def create(self, parsed_arguments):
        return pretty.PrettyReporter()
