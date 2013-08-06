from flow.shell_command import resource_types

import mock
import unittest


class MemoryUnitConversionTest(unittest.TestCase):
    def test_convert_larger_dest_unit(self):
        self.assertEqual(1, resource_types.convert_memory_value(1024, 'KiB',
            'MiB'))

    def test_convert_smaller_dest_unit(self):
        self.assertEqual(1024, resource_types.convert_memory_value(1, 'MiB',
            'KiB'))

    def test_convert_same_dest_unit(self):
        self.assertEqual(42, resource_types.convert_memory_value(42, 'KiB',
            'KiB'))


    def test_illegal_src_unit(self):
        with self.assertRaises(resource_types.ResourceException):
            resource_types.convert_memory_value(42, 'ILLEGAL', 'KiB')

    def test_illegal_dest_unit(self):
        with self.assertRaises(resource_types.ResourceException):
            resource_types.convert_memory_value(42, 'GiB', 'ILLEGAL')


    def test_illegal_value_type(self):
        with self.assertRaises(ValueError):
            resource_types.convert_memory_value('ILLEGAL', 'KiB', 'MiB')


class ResourceObjectTest(unittest.TestCase):
    def setUp(self):
        self.resource_types = {
            'request': {
                'cores': resource_types.ResourceType(
                    resource_class='IntegerResource'),
            },
            'reserve': {
                'cpu_time': resource_types.ResourceType(
                    resource_class='TimeResource', units='s'),
            },
            'limit': {
                'memory': resource_types.ResourceType(
                    resource_class='StorageResource', units='GiB'),
            },
        }

    def test_integer_resource(self):
        v = '42'
        r = resource_types.IntegerResource(v)

        self.assertEqual(int(v), r.value_in_units(None))

        with self.assertRaises(resource_types.ResourceException):
            r.value_in_units('MiB')

    def test_storage_resource_same_units(self):
        v = '7'
        r = resource_types.StorageResource(v, 'GiB')
        self.assertEqual(int(v), r.value_in_units('GiB'))

    def test_storage_resource_different_units(self):
        v = 1
        r = resource_types.StorageResource(v, 'GiB')
        self.assertEqual(1024, r.value_in_units('MiB'))

    def test_time_resource_same_units(self):
        v = '17'
        r = resource_types.TimeResource(v, 's')
        self.assertEqual(int(v), r.value_in_units('s'))

    def test_time_resource_illegal_units(self):
        v = '57'
        r = resource_types.TimeResource(v, 's')
        with self.assertRaises(resource_types.ResourceException):
            r.value_in_units('ms')


    def test_make_resource_objects(self):
        source = {
            'memory': 2,
        }
        expected_result = {
            'memory': resource_types.StorageResource(2, 'GiB'),
        }
        result = resource_types.make_resource_objects(source,
                self.resource_types['limit'])
        self.assertEqual(expected_result, result)

    def test_make_all_resource_objects(self):
        source = {
            'request': {
                'cores': 4,
            },
            'reserve': {
                'cpu_time': 3,
            },
            'limit': {
                'memory': 2,
            },
        }

        expected_result = {
            'request': {
                'cores': resource_types.IntegerResource(4),
            },
            'reserve': {
                'cpu_time': resource_types.TimeResource(3, 's'),
            },
            'limit': {
                'memory': resource_types.StorageResource(2, 'GiB'),
            },
        }

        result = resource_types.make_all_resource_objects(source,
                self.resource_types)
        self.assertEqual(expected_result, result)


if '__main__' == __name__:
    unittest.main()
