import logging

from . import base

LOG = logging.getLogger(__name__)


class GridSubmitResponder(base.Responder):
    def __init__(self, dispatcher, succeeded_routing_key=None,
            failed_routing_key=None, *args, **kwargs):
        self.dispatcher = dispatcher
        self.succeeded_routing_key = succeeded_routing_key
        self.failed_routing_key = failed_routing_key

        base.Responder.__init__(self, *args, **kwargs)

    def on_message(self, channel, basic_deliver, properties, workitem):
        LOG.debug("Got workitem %s", workitem)

        # XXX launch thinggy with wrapper
        job_id = self.dispatcher.launch_job(
                workitem['fields']['params']['command'],
                workitem['fields']['params']['arg'])

        return self.succeeded_routing_key, {'job_id': job_id}
