import flow.rabbit.filter.base


class NullFilter(flow.rabbit.filter.base.IFilter):
    def __call__(self, query_info):
        return query_info

    def header(self):
        return None
