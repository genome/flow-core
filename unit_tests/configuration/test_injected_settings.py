from injector import inject, Injector
from flow.configuration.settings.injector import setting
from flow.configuration.inject.settings import InjectedSettings

import mock
import unittest


class TestInjectedSettings(unittest.TestCase):
    def setUp(self):
        self.existing_value = mock.Mock()
        self.default = mock.Mock()

        self.settings = {
            'existing_key': self.existing_value,
        }


    def get_injector(self):
        return Injector(InjectedSettings(self.settings))

    def get_object(self, cls):
        i = self.get_injector()
        return i.get(cls)


    def test_existing_attribute_no_default(self):
        @inject(x=setting('existing_key'))
        class Foo(object): pass

        f = self.get_object(Foo)

        self.assertEqual(f.x, self.existing_value)

    def test_existing_attribute_with_default(self):
        @inject(x=setting('existing_key', self.default))
        class Foo(object): pass

        f = self.get_object(Foo)

        self.assertEqual(f.x, self.existing_value)

    def test_missing_attribute_with_default(self):
        @inject(x=setting('missing_key', self.default))
        class Foo(object): pass

        f = self.get_object(Foo)

        self.assertEqual(f.x, self.default)

    def test_missing_attribute_no_default(self):
        @inject(x=setting('missing_key'))
        class Foo(object): pass

        i = self.get_injector()

        self.assertRaises(KeyError, i.get, Foo)


    def test_no_settings_registered(self):
        @inject(x=setting('existing_key'))
        class Foo(object): pass

        i = Injector()

        self.assertRaises(TypeError, i.get, Foo)


if __name__ == "__main__":
    unittest.main()
