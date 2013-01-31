import logging

from flow.orchestrator.messages import *
from flow.petri.safenet import NotifyTransitionMessage, SetTokenMessage

LOG = logging.getLogger(__name__)

class OrchestratorClient(object):
    def __init__(self, broker=None,
            execute_node_routing_key=None,

            set_token_routing_key=None,
            notify_transition_routing_key=None,

            submit_flow_routing_key=None,
            submit_flow_success_routing_key=None,
            submit_flow_failure_routing_key=None,
            submit_flow_error_routing_key=None):

        self.broker = broker
        self.execute_node_routing_key        = execute_node_routing_key

        self.set_token_routing_key           = set_token_routing_key
        self.notify_transition_routing_key   = notify_transition_routing_key

        self.submit_flow_routing_key         = submit_flow_routing_key
        self.submit_flow_success_routing_key = submit_flow_success_routing_key
        self.submit_flow_failure_routing_key = submit_flow_failure_routing_key
        self.submit_flow_error_routing_key   = submit_flow_error_routing_key

    def set_token(self, net_key, place_idx, token_key=None):
        message = SetTokenMessage(net_key=net_key, place_idx=place_idx,
                token_key=token_key)
        self.broker.publish(self.set_token_routing_key, message)

    def notify_transition(self, net_key, transition_idx, place_idx):
        message = NotifyTransitionMessage(
                net_key=net_key,
                transition_idx=transition_idx,
                place_idx=place_idx)
        self.broker.publish(self.notify_transition_routing_key, message)

    def execute_node(self, node_key):
        message = ExecuteNodeMessage(node_key=node_key)
        self.broker.publish(self.execute_node_routing_key, message)

    def submit_flow(self, definition, return_identifier=None):
        message = FlowSubmitMessage(return_identifier=return_identifier,
                definition=definition,
                success_routing_key=self.submit_flow_success_routing_key,
                failure_routing_key=self.submit_flow_failure_routing_key,
                error_routing_key=self.submit_flow_error_routing_key)
        self.broker.publish(self.flow_submit_routing_key, message)
