from flow.rabbit.filter import null


class FilterFactory(object):
    def create(self, parsed_arguments):
        return null.NullFilter()
