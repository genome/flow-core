import flow.redisom as rom


class ActionBase(rom.Object):
    required_arguments = []

    args = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)

    def _on_create(self):
        for argname in self.required_arguments:
            if not argname in self.args:
                raise TypeError("In class %s: required argument %s missing" %
                        (self.__class__.__name__, argname))


    def execute(self, net, color_descriptor, active_tokens, service_interfaces):
        '''
        Returns (tokens, deferred)

        tokens should be put in the owning transition's output places

        deferred will callback when the action is actually completed
            (so that the request message can be acked)
        '''
        raise NotImplementedError("In class %s: execute not implemented"
                % self.__class__.__name__)


class BarrierActionBase(ActionBase):
    pass


class BasicActionBase(ActionBase):
    pass
