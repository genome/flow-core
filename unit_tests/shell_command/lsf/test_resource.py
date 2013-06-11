from flow.shell_command.lsf import resource
from flow.shell_command import resource as base_resource

import mock
import unittest


class LSFResourceBaseTest(unittest.TestCase):
    def setUp(self):
        class TestClass(resource.LSFResource):
            def set_select_component(self, *args, **kw):
                resource.LSFResource.set_select_component(self, *args, **kw)

            def set_reserve_component(self, *args, **kw):
                resource.LSFResource.set_reserve_component(self, *args, **kw)

        self.o = TestClass(name='foo')

    def test_reserve(self):
        with self.assertRaises(NotImplementedError):
            self.o.set_reserve_component(None, None, None)

    def test_select(self):
        with self.assertRaises(NotImplementedError):
            self.o.set_select_component(None, None, None)


class LSFResourceIgnoredTest(unittest.TestCase):
    def setUp(self):
        self.r = resource.LSFResourceIgnored()

        self.request = mock.MagicMock()
        self.strings = mock.MagicMock()
        self.spec = mock.MagicMock()

    def validate_unmodified(self):
        self.assertFalse(self.request.mock_calls)
        self.assertFalse(self.strings.mock_calls)
        self.assertFalse(self.spec.mock_calls)


    def test_reserve(self):
        self.r.set_reserve_component(self.request, self.strings, self.spec)
        self.validate_unmodified()

    def test_select(self):
        self.r.set_select_component(self.request, self.strings, self.spec)
        self.validate_unmodified()


class LSFResourceTestBase(object):
    def setUp(self):
        self.name = 'foo'
        self.operator = '<>'
        self.units = 'U'
        self.val = 'val'

        self.r = self.RESOURCE_CLASS(name=self.name,
                operator=self.operator, units=self.units)

        self.request = mock.MagicMock()
        self.spec = mock.Mock()
        self.spec.value_in_units.return_value = self.val

class LSFResourceViaStringTest(LSFResourceTestBase, unittest.TestCase):
    RESOURCE_CLASS = resource.LSFResourceViaString

    def test_reserve(self):
        rusage_strings = {}
        self.r.set_reserve_component(self.request, rusage_strings, self.spec)

        self.assertEqual('%s=%s' % (self.name, self.val),
                rusage_strings[self.name])

        self.assertFalse(self.request.mock_calls)
        self.spec.value_in_units.assert_called_once_with(self.units)

    def test_select(self):
        select_strings = {}
        self.r.set_select_component(self.request, select_strings, self.spec)

        self.assertEqual('%s%s%s' % (self.name, self.operator, self.val),
                select_strings[self.name])

        self.assertFalse(self.request.mock_calls)
        self.spec.value_in_units.assert_called_once_with(self.units)


class LSFResourceDirectRequestTest(LSFResourceTestBase, unittest.TestCase):
    RESOURCE_CLASS = resource.LSFResourceDirectRequest

    def test_reserve(self):
        rusage_strings = mock.MagicMock()
        self.r.set_reserve_component(self.request, rusage_strings, self.spec)

        self.assertFalse(rusage_strings.mock_calls)
        self.spec.value_in_units.assert_called_once_with(self.units)

        self.assertEqual(self.val, getattr(self.request, self.name))

    def test_select(self):
        select_strings = mock.MagicMock()
        self.r.set_select_component(self.request, select_strings, self.spec)

        self.assertFalse(select_strings.mock_calls)
        self.spec.value_in_units.assert_called_once_with(self.units)

        self.assertEqual(self.val, getattr(self.request, self.name))


class LSFLimitBaseTest(unittest.TestCase):
    def test_not_implemented(self):
        class TestClass(resource.LSFLimit):
            def set_limit(self, *args, **kw):
                return resource.LSFLimit.set_limit(self, *args, **kw)

        o = TestClass()
        with self.assertRaises(NotImplementedError):
            o.set_limit(None, None, None)


class LSFrlimitTest(unittest.TestCase):
    def setUp(self):
        self.rlimits = [0, 0, 0]
        self.option_index = 1
        self.units = mock.Mock()

        self.r = resource.LSFrlimit(self.option_index, units=self.units)

        self.request = mock.MagicMock()

        self.val = mock.Mock()
        self.spec = mock.Mock()
        self.spec.value_in_units.return_value = self.val

    def test_set_limit(self):
        self.r.set_limit(self.request, self.rlimits, self.spec)
        self.spec.value_in_units.assert_called_once_with(self.units)
        self.assertEqual(self.val, self.rlimits[self.option_index])


class SetResourcesTest(unittest.TestCase):
    def test_request_resources_reserve(self):
        request = mock.MagicMock()
        resources = {
            'reserve': {
                'memory': mock.MagicMock(),
            },
        }
        reserve_map = {
            'memory': mock.MagicMock(),
        }
        with mock.patch('flow.shell_command.lsf.resource.RESERVE_MAP',
                new=reserve_map):
            resource.set_request_resources(request, resources)

        reserve_map['memory'].set_reserve_component.assert_called_once_with(
                request, mock.ANY, resources['reserve']['memory'])

    def test_request_resources_request(self):
        request = mock.MagicMock()
        resources = {
            'request': {
                'memory': mock.MagicMock(),
            },
        }
        request_map = {
            'memory': mock.MagicMock(),
        }
        with mock.patch('flow.shell_command.lsf.resource.SELECT_MAP',
                new=request_map):
            resource.set_request_resources(request, resources)

        request_map['memory'].set_select_component.assert_called_once_with(
                request, mock.ANY, resources['request']['memory'])

    def test_request_resources_illegal_reserve(self):
        request = mock.MagicMock()
        resources = {
            'reserve': {
                'MISSING_VALUE': mock.Mock(),
            },
        }
        with self.assertRaises(resource.ResourceException):
            resource.set_request_resources(request, resources)

    def test_request_resources_illegal_request(self):
        request = mock.MagicMock()
        resources = {
            'request': {
                'MISSING_VALUE': mock.Mock(),
            },
        }
        with self.assertRaises(resource.ResourceException):
            resource.set_request_resources(request, resources)


class MakeRlimitsTest(unittest.TestCase):
    def test_make_rLimits_normal(self):
        request = mock.MagicMock()
        specs = {
                'mock': mock.MagicMock(),
        }

        limit_map = {
            'mock': mock.MagicMock(),
        }
        with mock.patch('flow.shell_command.lsf.resource.LIMIT_MAP',
                new=limit_map):
            rlimits = resource.make_rLimits(request, specs)

        limit_map['mock'].set_limit.assert_called_once_with(
                request, mock.ANY, specs['mock'])

    def test_make_rLimits_exception(self):
        request = mock.MagicMock()

        specs = {'MISSING_VALUE': mock.MagicMock()}
        with self.assertRaises(resource.ResourceException):
            resource.make_rLimits(request, specs)


class RusageStringTest(unittest.TestCase):
    def test_make_rusage_string(self):
        select_strings = ['a', 'b', 'c']
        rusage_strings = ['d', 'e', 'f']

        expected_string = 'select[a && b && c] rusage[d:e:f]'
        self.assertEqual(expected_string, resource.make_rusage_string(
            select_strings, rusage_strings))


if '__main__' == __name__:
    unittest.main()
