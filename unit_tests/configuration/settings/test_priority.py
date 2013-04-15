from flow.configuration.settings.priority import PrioritySettings
from flow.configuration.settings.cache import CacheSettings
import mock
import unittest

class TestPrioritySettings(unittest.TestCase):
    def add_delegate(self, c, path):
        value = mock.Mock()

        subconf = CacheSettings()
        subconf.set(path, value)

        c.append(subconf)

        return value

    def test_empty_getitem_raises(self):
        c = PrioritySettings()

        self.assertRaises(KeyError, c.__getitem__, 'a')

    def test_empty_get(self):
        c = PrioritySettings()

        default = mock.Mock()
        self.assertEqual(default, c.get('a', default))


    def test_get_single_delegate_missing(self):
        c = PrioritySettings()
        value = self.add_delegate(c, 'a.b.c')

        default = mock.Mock()
        self.assertEqual(default, c.get('c.b.a', default))

    def test_get_single_delegate_exists(self):
        c = PrioritySettings()
        value = self.add_delegate(c, 'a.b.c')

        self.assertEqual(value, c.get('a.b.c'))


    def test_get_multiple_delegates_missing(self):
        c = PrioritySettings()
        value1 = self.add_delegate(c, 'a.b.c')
        value2 = self.add_delegate(c, 'a.c.b')

        default = mock.Mock()
        self.assertEqual(default, c.get('c.b.a', default))

    def test_get_multiple_delegates_exists_no_shadow(self):
        c = PrioritySettings()
        value1 = self.add_delegate(c, 'a.b.c')
        value2 = self.add_delegate(c, 'a.c.b')

        self.assertEqual(value1, c.get('a.b.c'))
        self.assertEqual(value2, c.get('a.c.b'))

    def test_get_multiple_delegates_exists_shadow(self):
        c = PrioritySettings()
        value1 = self.add_delegate(c, 'a.b.c')
        value2 = self.add_delegate(c, 'a.b.c')

        self.assertEqual(value2, c.get('a.b.c'))


if __name__ == "__main__":
    unittest.main()
