from flow.shell_command import factory

import mock
import sys
import unittest


test_module = sys.modules[__name__]


class TargetObject(object):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2


class FactoryTest(unittest.TestCase):
    def test_build_object_success(self):
        definition = {
            'class': 'TargetObject',
            'arg1': mock.Mock(),
            'arg2': mock.Mock(),
        }

        obj = factory.build_object(definition, test_module)

        self.assertIsInstance(obj, TargetObject)
        self.assertIs(definition['arg1'], obj.arg1)
        self.assertIs(definition['arg2'], obj.arg2)

    def test_build_object_failure(self):
        definition = {
            'class': 'TargetObject',
            'arg1': mock.Mock(),
        }

        with self.assertRaises(TypeError):
            factory.build_object(definition, test_module)

    def test_build_objects(self):
        definitions = {
            'a': {
                'class': 'TargetObject',
                'arg1': mock.Mock(),
                'arg2': mock.Mock(),
            },
            'b': {
                'class': 'TargetObject',
                'arg1': mock.Mock(),
                'arg2': mock.Mock(),
            },
        }

        objs = factory.build_objects(definitions, test_module)

        for name, obj in objs.iteritems():
            self.assertIsInstance(obj, TargetObject)
            self.assertIs(definitions[name]['arg1'], obj.arg1)
            self.assertIs(definitions[name]['arg2'], obj.arg2)


if '__main__' == __name__:
    unittest.main()
