import logging
import time
import uuid

from flow.petri.safenet import SafeNet
from flow.orchestrator.redisom import get_object, invoke_instance_method

from flow.orchestrator.messages import NodeStatusRequestMessage, NodeStatusResponseMessage

LOG = logging.getLogger(__name__)

class PetriSetTokenHandler(object):
    def __init__(self, redis=None, services=None):
        self.redis = redis
        self.services = services

    def __call__(self, message):
        try:
            net = SafeNet(self.redis, message.net_key)
            net.set_token(message.place_idx, message.token_key,
                    services=self.services)
        except Exception as e:
            LOG.error('Handler (%s) failed to add tokens to net %s place %d: %s'
                    % (self, message.net_key, message.place_idx, str(e)))
            raise e


class PetriNotifyTransitionHandler(object):
    def __init__(self, redis=None, services=None):
        self.redis = redis
        self.services = services

    def __call__(self, message):
        try:
            net = SafeNet(self.redis, message.net_key)
            net.notify_transition(message.transition_idx, message.place_idx,
                    services=self.services)
        except Exception as e:
            LOG.error('Handler (%s) failed to execute transition %s: %s'
                    % (self, message.transition_idx, str(e)))
            raise e


class MethodDescriptorHandler(object):
    def __init__(self, services=None, callback_name=None,
            queue_name=None, storage=None):
        self.callback_name = callback_name
        self.queue_name = queue_name
        self.storage = storage

    def __call__(self, message):
        try:
            method_descriptor = message.return_identifier[self.callback_name]
        except KeyError:
            LOG.exception('Failed to get method descriptor (%s) from message',
                    self.callback_name)
            raise

        try:
            invoke_instance_method(self.storage, method_descriptor,
                    services=self.services)

        except:
            LOG.error('Handler (%s) failed to execute method descriptor: %s',
                    self, method_descriptor)
            raise

class ExecuteNodeHandler(object):
    def __init__(self, services=None, queue_name=None, storage=None):
        self.services = services
        self.queue_name = queue_name
        self.storage = storage

    def __call__(self, message):
        print message.node_key # TODO remove after benchmarking?
        LOG.debug('Executing node: %s' % message.node_key)
        node = get_object(self.storage, message.node_key)
        node.execute(self.services)


class NodeStatusRequestHandler(object):
    def __init__(self, services=None, queue_name=None, storage=None):
        self.services = services
        self.queue_name = queue_name
        self.storage = storage

    def __call__(self, message):
        response_message = NodeStatusResponseMessage(
                node_key=message.node_key, status='unknown node')
        try:
            node = get_object(self.storage, message.node_key)
        except:
            LOG.warning('Status requested for unknown node (%s)',
                    message.node_key)
        else:
            if node:
                status = str(node.status)
                if status:
                    response_message.status = status
                else:
                    response_message.status = 'unknown status'

        self.broker.publish(message.response_routing_key, response_message)


class NodeStatusResponseHandler(object):
    def __init__(self, broker=None, polling_interval=5, node_key=None,
            request_routing_key=None, response_routing_key=None,
            queue_name=None, storage=None):
        self.broker = broker
        self.polling_interval = polling_interval
        self.node_key = node_key
        self.request_routing_key = request_routing_key
        self.response_routing_key = response_routing_key

        self.services = services
        self.queue_name = queue_name
        self.storage = storage

        identifier = uuid.uuid4().hex
        self.response_routing_key = 'flow.status.response.%s' % identifier
        self.response_queue = 'flow_status_response_%s' % identifier

    def __call__(self, message):
        status = message.status
        LOG.debug('Status for node (%s) is %s', message.node_key, status)
        if status == "success":
            self.broker.exit(0)
        elif status == "failure":
            self.broker.exit(1)
        else:
            time.sleep(self.polling_interval)
            self.send_request()

    def send_request(self):
        LOG.debug('Sending status request for node (%s).', self.node_key)
        request_message = NodeStatusRequestMessage(node_key=self.node_key,
                response_routing_key=self.response_routing_key)
        self.broker.publish(self.request_routing_key, request_message)
