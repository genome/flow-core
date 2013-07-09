from flow.petri_net.actions.base import BasicActionBase
from flow.util.containers import head
from twisted.internet import defer

import flow.redisom as rom


class RemoveDataAction(BasicActionBase):
    required_args = ['fields']

    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        assert len(active_tokens) == 1

        old_token = net.token(head(active_tokens))

        data = old_token.data.value
        for field in self.args['fields']:
            data.pop(field, None)

        new_token = net.create_token(color=color_descriptor.color,
                color_group_idx=color_descriptor.group.idx, data=data)

        return [new_token], defer.succeed(None)
