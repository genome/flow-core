from flow.petri_net.actions.base import BarrierActionBase, BasicActionBase
from flow.petri_net import lua
from flow.util.containers import head
from twisted.internet import defer

import flow.redisom as rom


class MergeMixin(object):
    def merge_data(self, net, dest_token, active_tokens):
        keys = [dest_token.data.key]
        keys.extend(net.token(t).data.key for t in active_tokens)
        rv = self._merge_hashes_script(keys=keys)
        if rv[0] != 0:
            raise RuntimeError('Failed to merge token data for tokens: %s'
                    % [t for t in active_tokens])



class BasicMergeAction(BasicActionBase, MergeMixin):
    _merge_hashes_script = rom.Script(lua.load('merge_hashes'))

    def execute(self, net, color_descriptor,
            active_tokens, service_interfaces):
        if len(active_tokens) == 1:
            new_token_idx = head(active_tokens)
            new_token = net.token(new_token_idx)
        else:
            new_token = net.create_token(color=color_descriptor.color,
                    color_group_idx=color_descriptor.group.idx)
            self.merge_data(net, new_token, active_tokens)

        return [new_token], defer.succeed(None)


class BarrierMergeAction(BarrierActionBase, MergeMixin):
    _merge_hashes_script = rom.Script(lua.load('merge_hashes'))

    def execute(self, net, color_descriptor,
            active_tokens, service_interfaces):
        raise NotImplementedError(":(")
