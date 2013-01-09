import logging

from flow.orchestrator.redisom import get_object

LOG = logging.getLogger(__name__)

class OrchestratorNodeHandler(object):
    def __init__(self, redis=None, services=None, callback_name=None):
        assert(callback_name in
                set(['on_ready', 'on_start', 'on_success', 'on_failure']))
        self.redis = redis
        self.services = services
        self.callback_name = callback_name

    def message_handler(self, message):
        node = get_object(self.redis, message.return_identifier)
        callback = getattr(node, self.callback_name)
        callback(self.services)
