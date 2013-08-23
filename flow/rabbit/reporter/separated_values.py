import csv
import flow.rabbit.reporter.base
import sys


class CSVReporter(flow.rabbit.reporter.base.IReporter):
    def __call__(self, result):
        writer = csv.DictWriter(sys.stdout, self.get_field_names(result))
        writer.writeheader()
        for row in result:
            writer.writerow(row)

    def get_field_names(self, result):
        try:
            row = result[0]
            return row.keys()
        except:
            return []
