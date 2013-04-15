import injector

def initialize_injector(settings, command):
    i = injector.Injector(command.injector_modules)
    i.binder.bind(injector.Injector, i)

    i.binder.bind_settings(settings)

    return i
