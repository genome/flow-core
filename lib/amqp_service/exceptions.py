class ResponderTaskFailed(RuntimeError):
    def __init__(self, **kwargs):
        for k, v in kwargs.iteritems():
            self.setattr(k, v)
