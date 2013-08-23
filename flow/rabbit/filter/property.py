import flow.rabbit.filter.base


class PropertyFilterBase(flow.rabbit.filter.base.IFilter):
    def __init__(self, *property_names):
        self.property_names = property_names


class BlockPropertyFilter(PropertyFilterBase):
    def __call__(self, query_info):
        return {k: v for k, v in query_info.iteritems()
                if k not in self.property_names}


class SelectPropertyFilter(PropertyFilterBase):
    def __call__(self, query_info):
        return {k: v for k, v in query_info.iteritems()
                if k in self.property_names}
