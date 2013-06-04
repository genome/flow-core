from flow.petri_net.color import ColorDescriptor

import flow.redisom as rom


class Token(rom.Object):
    data = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)

    net_key = rom.Property(rom.String)

    color = rom.Property(rom.Int)
    color_group_idx = rom.Property(rom.Int)
    index = rom.Property(rom.Int)

    @property
    def color_group(self):
        return self.net.color_group(self.color_group_idx.value)

    @property
    def color_descriptor(self):
        return ColorDescriptor(self.color.value, self.color_group)

    @property
    def net(self):
        return rom.get_object(self.connection, self.net_key)
