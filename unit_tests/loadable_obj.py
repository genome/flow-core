import flow.redisom as rom

class LoadableObj(rom.Object):
    ascalar = rom.Property(rom.String)

print "Loaded LoadableObj in module unit_tests.loadable_obj"
