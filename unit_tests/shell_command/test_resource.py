from flow.shell_command import resource

import mock
import unittest


class MemoryUnitConversionTest(unittest.TestCase):
    def test_convert_larger_dest_unit(self):
        self.assertEqual(1, resource.convert_memory_value(1024, 'KiB', 'MiB'))

    def test_convert_smaller_dest_unit(self):
        self.assertEqual(1024, resource.convert_memory_value(1, 'MiB', 'KiB'))

    def test_convert_same_dest_unit(self):
        self.assertEqual(42, resource.convert_memory_value(42, 'KiB', 'KiB'))


    def test_illegal_src_unit(self):
        with self.assertRaises(resource.ResourceException):
            resource.convert_memory_value(42, 'ILLEGAL', 'KiB')

    def test_illegal_dest_unit(self):
        with self.assertRaises(resource.ResourceException):
            resource.convert_memory_value(42, 'GiB', 'ILLEGAL')


    def test_illegal_value_type(self):
        with self.assertRaises(ValueError):
            resource.convert_memory_value('ILLEGAL', 'KiB', 'MiB')


class ResourceObjectTest(unittest.TestCase):
    def test_integer_resource(self):
        v = mock.Mock()
        r = resource.IntegerResource(v)

        self.assertEqual(v, r.value_in_units(None))

        with self.assertRaises(resource.ResourceException):
            r.value_in_units('MiB')

    def test_storage_resource_same_units(self):
        v = mock.Mock()
        r = resource.StorageResource(v)
        self.assertEqual(v, r.value_in_units('GiB'))

    def test_storage_resource_different_units(self):
        v = 1
        r = resource.StorageResource(v)
        self.assertEqual(1024, r.value_in_units('MiB'))

    def test_time_resource_same_units(self):
        v = mock.Mock()
        r = resource.TimeResource(v)
        self.assertEqual(v, r.value_in_units('s'))

    def test_time_resource_illegal_units(self):
        v = mock.Mock()
        r = resource.TimeResource(v)
        with self.assertRaises(resource.ResourceException):
            r.value_in_units('ms')


    def test_make_resource_objects(self):
        source = {
            'cores': 4,
            'cpu_time': 3,
            'memory': 2,
        }
        expected_result = {
            'cores': resource.IntegerResource(4),
            'cpu_time': resource.TimeResource(3),
            'memory': resource.StorageResource(2),
        }
        result = resource.make_resource_objects(source)
        self.assertItemsEqual(expected_result, result)

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
                'cores': resource.IntegerResource(4),
            },
            'reserve': {
                'cpu_time': resource.TimeResource(3),
            },
            'limit': {
                'memory': resource.StorageResource(2),
            },
        }

        result = resource.make_all_resource_objects(source)
        self.assertItemsEqual(expected_result, result)


if '__main__' == __name__:
    unittest.main()
