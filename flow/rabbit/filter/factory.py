from flow.rabbit.filter import null
from flow.rabbit.filter import property as prop_filters


class FilterFactory(object):
    def create(self, parsed_arguments):
        if parsed_arguments.select_property_names:
            return prop_filters.SelectPropertyFilter(
                    *parsed_arguments.select_property_names)
        elif parsed_arguments.blocked_property_names:
            return prop_filters.BlockPropertyFilter(
                    *parsed_arguments.blocked_property_names)

        return null.NullFilter()
