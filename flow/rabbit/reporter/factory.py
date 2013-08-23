from flow.rabbit.reporter import pretty, separated_values

_REPORT_TYPES = {
    'csv': separated_values.CSVReporter,
}


class ReporterFactory(object):
    def create(self, parsed_arguments):
        cls = _REPORT_TYPES.get(parsed_arguments.report_type,
                pretty.PrettyReporter)
        return cls()
