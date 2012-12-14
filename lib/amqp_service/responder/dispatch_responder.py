import json
import logging

from . import base

LOG = logging.getLogger(__name__)


class DispatchResponder(base.Responder):
    def __init__(self, dispatcher, succeeded_routing_key=None,
            failed_routing_key=None, *args, **kwargs):
        self.dispatcher = dispatcher
        self.succeeded_routing_key = succeeded_routing_key
        self.failed_routing_key = failed_routing_key

        base.Responder.__init__(self, *args, **kwargs)

    def on_message(self, channel, basic_deliver, properties, input_data):
        LOG.debug("Got input_data %s", input_data)

        try:
            command = input_data['command']
        except KeyError:
            LOG.error("command not specified")
            raise

        try:
            return_identifier = input_data['return_identifier']
        except KeyError:
            LOG.error("return_identifier not specified")
            raise

        arguments = input_data.get('arguments', [])
        wrapper = input_data.get('wrapper')
        wrapper_arguments = input_data.get('wrapper_arguments', [])
        dispatcher_options = input_data.get('dispatcher_options', {})

        success, dispatch_result = self.dispatcher.launch_job(
                command, arguments=arguments,
                wrapper=wrapper, wrapper_arguments=wrapper_arguments,
                **dispatcher_options)

        if success:
            routing_key = self.succeeded_routing_key
        else:
            routing_key = self.failed_routing_key

        result = {'return_identifier': return_identifier,
                  'dispatch_result': dispatch_result}

        return routing_key, result
