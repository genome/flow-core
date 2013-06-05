from flow.petri_net.actions.base import BarrierActionBase, BasicActionBase
from twisted.internet import defer


class BasicClearDataAction(BasicActionBase):
    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        token = net.create_token(color=color_descriptor.color,
                color_group_idx=color_descriptor.color_group.idx)
        return [token], defer.succeed(None)

class BarrierClearDataAction(BarrierActionBase):
    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        cg = color_descriptor.color_group
        tokens = []
        for color in cg.color_iter:
            tokens.append(net.create_token(color=color,
                color_group_idx=cg.idx))

        return tokens, defer.succeed(None)
