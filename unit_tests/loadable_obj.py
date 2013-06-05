import flow.redisom as rom

class LoadableObj(rom.Object):
    ascalar = rom.Property(rom.String)
