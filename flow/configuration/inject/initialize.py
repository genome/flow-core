from flow.configuration.inject.settings import InjectedSettings

import copy
import injector


INJECTOR = injector.Injector()


def initialize_injector(settings, command_class):
    add_modules(INJECTOR, InjectedSettings(settings),
            *command_class.injector_modules)

    return INJECTOR

def add_modules(injector, *modules):
    for module in modules:
        if isinstance(module, type):
            module = module()

        module(injector.binder)

def reset_injector():
    global INJECTOR
    INJECTOR = injector.Injector()
