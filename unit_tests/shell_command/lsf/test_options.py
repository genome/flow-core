from flow.shell_command.lsf import options
from pythonlsf import lsf

import mock
import unittest


class LSFOptionTest(unittest.TestCase):
    def setUp(self):
        self.request = mock.Mock()
        self.request.options = 0
        self.request.options2 = 0
        self.request.options3 = 0


    def test_no_flag(self):
        name = 'sample_name'
        value = 'sample_value'

        o = options.LSFOption(name=name)
        o.set_option(self.request, value)

        self.assertEqual(value, getattr(self.request, name))

    def test_flag(self):
        name = 'sample_name'
        value = 'sample_value'
        flag = 8

        o = options.LSFOption(name=name, flag=flag)
        o.set_option(self.request, value)

        self.assertEqual(flag, self.request.options)

    def test_option_suffix(self):
        name = 'sample_name'
        value = 'sample_value'
        flag = 8

        o = options.LSFOption(name=name, flag=flag, option_set=3)
        o.set_option(self.request, value)

        self.assertEqual(flag, self.request.options3)

    def test_type(self):
        name = 'sample_name'
        value = 12.1

        o = options.LSFOption(name=name, type=float)
        o.set_option(self.request, value)

        self.assertEqual(value, getattr(self.request, name))
        self.assertEqual(float, type(getattr(self.request, name)))


if '__main__' == __name__:
    unittest.main()
