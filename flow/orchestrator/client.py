import logging

from flow.orchestrator.messages import *

LOG = logging.getLogger(__name__)

class OrchestratorClient(object):
    def __init__(self, broker=None,
            execute_node_routing_key=None,

            submit_flow_routing_key=None,
            submit_flow_success_routing_key=None,
            submit_flow_failure_routing_key=None,
            submit_flow_error_routing_key=None):

        self.broker = broker
        self.execute_node_routing_key        = execute_node_routing_key

        self.submit_flow_routing_key         = submit_flow_routing_key
        self.submit_flow_success_routing_key = submit_flow_success_routing_key
        self.submit_flow_failure_routing_key = submit_flow_failure_routing_key
        self.submit_flow_error_routing_key   = submit_flow_error_routing_key

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
