import importlib

def load_object(descriptor):
    module_name, object_name = descriptor.split(':')
    module = importlib.import_module(module_name)

    return getattr(module, object_name)
