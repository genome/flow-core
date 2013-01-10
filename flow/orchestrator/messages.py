from flow.protocol.message import Message

__all__ = ['FlowSubmitMessage', 'ExecuteNodeMessage']

class FlowSubmitMessage(Message):
    required_fields = {
            'return_identifier': object,
            'definition': object,

            'success_routing_key': basestring,
            'failure_routing_key': basestring,
            'error_routing_key': basestring,
    }


class ExecuteNodeMessage(Message):
    required_fields = {
            'node_key': str,
    }
