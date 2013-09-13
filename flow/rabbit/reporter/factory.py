from flow.rabbit.reporter import pretty, separated_values, yaml_report

_REPORT_TYPES = {
    'csv': separated_values.CSVReporter,
    'yaml': yaml_report.YamlReporter,
}


class ReporterFactory(object):
    def create(self, parsed_arguments):
        cls = _REPORT_TYPES.get(parsed_arguments.report_type,
                pretty.PrettyReporter)
        return cls()
