import json
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
                workitem['fields']['params']['arg'],
                wrapper='/gscuser/mburnett/ruote/python-grid-service/bin/basic_command_wrapper.py',
                wrapper_args=['--amqp_username guest --amqp_password guest --return_packet',
                    "'%s'" % json.dumps(workitem), ' --amqp_exchange lsf --success_routing_key',
                    'job.succeed', '--failure_routing_key', 'job.fail']),

        return self.succeeded_routing_key, {'workitem': workitem, 'grid_job_id': job_id}
