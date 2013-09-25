from flow.configuration.inject.settings import InjectedSettings

import injector


INJECTOR = injector.Injector()


def initialize_injector(settings, command_class=None):
    if command_class:
        args = command_class.injector_modules
    else:
        args = []

    add_modules(INJECTOR, InjectedSettings(settings), *args)

    return INJECTOR

def add_modules(inj, *modules):
    for module in modules:
        if isinstance(module, type):
            module = module()

        module(inj.binder)

def reset_injector():
    global INJECTOR
    INJECTOR = injector.Injector()
