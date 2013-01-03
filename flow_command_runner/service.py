import logging

LOG = logging.getLogger(__name__)

class DispatchService(object):
    def __init__(self, executor, exchange_manager, **publish_properties):
        self.executor = executor
        self.exchange_manager = exchange_manager
        self.publish_properties = publish_properties

    def bad_data_handler(self, properties, body, ack_callback, reject_callback):
        LOG.debug('Got bad data, properties = %s: %s', properties, body)
        reject_callback()

    def message_handler(self, properties, input_data,
            ack_callback, reject_callback):
        command_line = _get_required(input_data, 'command_line')
        return_identifier = _get_required(input_data, 'return_identifier')

        success_routing_key = _get_required(input_data, 'success_routing_key')
        failure_routing_key = _get_required(input_data, 'failure_routing_key')
        error_routing_key = _get_required(input_data, 'error_routing_key')

        executor_options = input_data.get('executor_options', {})

        # XXX These really belong inside executor options
        environment = input_data.get('environment', {})
        working_directory = input_data.get('working_directory', None)
        stdout = input_data.get('stdout')
        stderr = input_data.get('stderr')

        try:
            success, dispatch_result = self.executor.launch_job(
                    command_line, working_directory=working_directory,
                    environment=environment, stderr=stderr, stdout=stdout,
                    **executor_options)

            if success:
                routing_key = success_routing_key
            else:
                routing_key = failure_routing_key

        except RuntimeError as e:
            routing_key = error_routing_key
            dispatch_result = str(e)

        result = {'return_identifier': return_identifier,
                  'dispatch_result': dispatch_result}

        self.exchange_manager.publish(routing_key, result,
                **self.publish_properties)

        ack_callback()

def _get_required(input_data, name):
    try:
        return input_data[name]
    except KeyError:
        LOG.error("required message key '%s' not specified", name)
        raise
