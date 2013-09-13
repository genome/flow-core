import yaml
import flow.rabbit.reporter.base


class YamlReporter(flow.rabbit.reporter.base.IReporter):
    def __call__(self, result):
        print yaml.safe_dump(result)
