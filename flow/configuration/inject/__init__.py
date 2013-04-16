import injector

def initialize_injector(settings, command_class):
    i = injector.Injector(command_class.injector_modules)
    i.binder.bind(injector.Injector, i)

    i.binder.bind_settings(settings)

    return i
