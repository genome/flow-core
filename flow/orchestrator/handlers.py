import logging

from flow.orchestrator.redisom import get_object, invoke_instance_method

LOG = logging.getLogger(__name__)

class MethodDescriptorHandler(object):
    def __init__(self, redis=None, services=None, callback_name=None):
        self.redis = redis
        self.services = services
        self.callback_name = callback_name


    def __call__(self, message):
        try:
            method_descriptor = message.return_identifier[self.callback_name]
        except KeyError:
            LOG.exception('Failed to get method descriptor (%s) from message',
                    self.callback_name)
            raise

        try:
            invoke_instance_method(self.redis, method_descriptor,
                    services=self.services)

        except:
            LOG.error('Handler (%s) failed to execute method descriptor: %s',
                    self, method_descriptor)
            raise

class ExecuteNodeHandler(object):
    def __init__(self, redis=None, services=None):
        self.redis = redis
        self.services = services

    def __call__(self, message):
        print message.node_key
        node = get_object(self.redis, message.node_key)
        node.execute(self.services)
