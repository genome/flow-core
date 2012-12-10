import logging

from . import base

from pprint import pprint

LOG = logging.getLogger(__name__)


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

        return 'do_some_succeeded_thing', {'job_id': job_id}
