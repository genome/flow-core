from base import BarrierActionBase, BasicActionBase
from flow.petri_net import lua

import flow.redisom as rom


class MergeMixin(object):
    def merge_data(self, dest_token, active_tokens):
        raise NotImplementedError("Work, work.")


class BasicMergeAction(BasicActionBase, MergeMixin):
    _merge_hashes_script = rom.Script(lua.load('merge_hashes'))

    def execute(self, net, color_descriptor,
            active_tokens, service_interfaces):
        if len(active_tokens) == 1:
            return iter(active_tokens).next()
        else:
            new_token = net.create_token(color=color_descriptor.color,
                    color_group_idx=color_descriptor.color_group.idx)
            self.merge_data(new_token, active_tokens)
            return new_token


class BarrierMergeAction(BarrierActionBase, MergeMixin):
    _merge_hashes_script = rom.Script(lua.load('merge_hashes'))

    def execute(self, net, color_descriptor,
            active_tokens, service_interfaces):
        raise NotImplementedError(":(")
