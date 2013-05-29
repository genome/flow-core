import flow.redisom as rom


class Place(rom.Object):
    name = rom.Property(rom.String)
    arcs_in = rom.Property(rom.List, value_decoder=int, value_encoder=int)
    arcs_out = rom.Property(rom.List, value_decoder=int, value_encoder=int)
    index = rom.Property(rom.Int)
    first_token_timestamp = rom.Property(rom.Timestamp)
