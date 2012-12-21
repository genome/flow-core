import json
import logging

from . import base

LOG = logging.getLogger(__name__)


class DispatchResponder(base.Responder):
    def __init__(self, dispatcher, *args, **kwargs):
        self.dispatcher = dispatcher

        base.Responder.__init__(self, *args, **kwargs)

    def on_message(self, channel, basic_deliver, properties, input_data):
        LOG.debug("Got input_data for message %s", basic_deliver)

        command_line = _get_required(input_data, 'command_line')
        return_identifier = _get_required(input_data, 'return_identifier')

        success_routing_key = _get_required(input_data, 'success_routing_key')
        failure_routing_key = _get_required(input_data, 'failure_routing_key')
        error_routing_key = _get_required(input_data, 'error_routing_key')

        dispatcher_options = input_data.get('dispatcher_options', {})

        # XXX These really belong inside dispatcher options
        environment = input_data.get('environment', {})
        working_directory = input_data.get('working_directory', None)
        stdout = input_data.get('stdout')
        stderr = input_data.get('stderr')

        try:
            success, dispatch_result = self.dispatcher.launch_job(
                    command_line, working_directory=working_directory,
                    environment=environment, stderr=stderr, stdout=stdout,
                    **dispatcher_options)

            if success:
                routing_key = success_routing_key
            else:
                routing_key = failure_routing_key

        except RuntimeError as e:
            routing_key = error_routing_key
            dispatch_result = str(e)

        result = {'return_identifier': return_identifier,
                  'dispatch_result': dispatch_result}

        return routing_key, result

def _get_required(input_data, name):
    try:
        return input_data[name]
    except KeyError:
        LOG.error("required message key '%s' not specified", name)
        raise
