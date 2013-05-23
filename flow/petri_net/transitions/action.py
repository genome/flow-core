import flow.redisom as rom


class TransitionAction(rom.Object):
    required_arguments = []

    name = rom.Property(rom.String)
    args = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)

    def _on_create(self):
        for argname in self.required_arguments:
            if not argname in self.args:
                raise TypeError("In class %s: required argument %s missing" %
                        (self.__class__.__name__, argname))

    def input_data(self, active_tokens_key, net):
        pass

    def tokens(self, active_tokens_key):
        keys = self.connection.lrange(active_tokens_key, 0, -1)
        return [rom.get_object(self.connection, x) for x in keys]

    def active_color(self, active_tokens_key):
        first_key = self.connection.lrange(active_tokens_key, 0, 0)
        if not first_key:
            return None

        try:
            token = rom.get_object(self.connection, first_key[0])
            return token.color_idx.value
        except rom.NotInRedisError:
            return None

    def execute(self, active_tokens_key, net, service_interfaces):
        """
        Returns a deferred that will callback (with either a new_token or
        None) once the action has been completed.
        """
        raise NotImplementedError("In class %s: execute not implemented" %
                self.__class__.__name__)
