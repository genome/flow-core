import logging

from . import base

from pprint import pprint

LOGGER = logging.getLogger(__name__)


class GridSubmitResponder(base.Responder):
    def __init__(self, dispatcher, *args, **kwargs):
        self.dispatcher = dispatcher
        base.Responder.__init__(self, *args, **kwargs)

    def on_message(self, channel, basic_deliver, properties, workitem):
        pprint(workitem)

        # XXX launch thinggy with wrapper
        job_id = self.dispatcher.launch_job(
                workitem['fields']['params']['command'],
                workitem['fields']['params']['arg'])

        return {'job_id': job_id}
