from flow.configuration.settings.cache import CacheSettings

import mock
import unittest


class TestPathCache(unittest.TestCase):
    def test_getitem_trivial_not_exists(self):
        c = CacheSettings()

        self.assertRaises(KeyError, c.__getitem__, 'a')

    def test_trivial_not_exists(self):
        c = CacheSettings()

        default = mock.Mock()
        self.assertEqual(default, c.get('a', default))

    def test_getitem_not_exists(self):
        a = mock.Mock()
        d = {'a': a}

        c = CacheSettings()

        c.replace(d)

        self.assertRaises(KeyError, c.__getitem__, 'b')

    def test_get_not_exists(self):
        a = mock.Mock()
        d = {'a': a}

        c = CacheSettings()

        c.replace(d)

        default = mock.Mock()
        self.assertEqual(default, c.get('b', default))

    def test_getitem_deep_not_exists(self):
        ab = mock.Mock()
        d = {'a': {'b': ab}}

        c = CacheSettings()

        c.replace(d)

        self.assertRaises(KeyError, c.__getitem__, 'a.c')

    def test_get_deep_not_exists(self):
        ab = mock.Mock()
        d = {'a': {'b': ab}}

        c = CacheSettings()

        c.replace(d)

        default = mock.Mock()
        self.assertEqual(default, c.get('a.c', default))


    def test_get_trivial_exists(self):
        a = mock.Mock()
        d = {'a': a}

        c = CacheSettings()

        c.replace(d)

        self.assertEqual(a, c.get('a'))

    def test_getitem_deep(self):
        ab = mock.Mock()
        d = {'a': {'b': ab}}

        c = CacheSettings()

        c.replace(d)

        self.assertEqual(ab, c['a.b'])


    def test_get_deep(self):
        ab = mock.Mock()
        d = {'a': {'b': ab}}

        c = CacheSettings()

        c.replace(d)

        self.assertEqual(ab, c.get('a.b'))


    def test_set(self):
        c = CacheSettings()
        ab = mock.Mock()
        c.set('a.b', ab)

        expected_dict = {'a': {'b': ab}}

        self.assertDictEqual(expected_dict, c.to_dict())

    def test_set_returns_previous(self):
        c = CacheSettings()
        ab1 = mock.Mock()
        ab2 = mock.Mock()
        c.set('a.b', ab1)

        self.assertEqual(ab1, c.set('a.b', ab2))
        self.assertEqual(ab2, c.get('a.b'))


    def test_set_get(self):
        c = CacheSettings()

        path = 'a.b.c'
        value = mock.Mock()

        c.set(path, value)

        self.assertEqual(value, c.get(path))


if __name__ == "__main__":
    unittest.main()
