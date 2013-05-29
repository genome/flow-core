import os.path

BASE_PATH = os.path.dirname(__file__)

def load(*module_names):
    parts = []
    for mn in module_names:
        filename = os.path.join(BASE_PATH, '%s.lua' % mn)
        parts.append(open(filename).read())

    return '\n'.join(parts)
