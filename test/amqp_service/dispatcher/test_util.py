import unittest
try:
    from unittest import mock
except:
    import mock

import os
from uuid import uuid4
from amqp_service.dispatcher import util


class EnvironmentContextManagerTest(unittest.TestCase):
    def test_no_contamination(self):
        name = "test_uuid_%s" % uuid4().hex
        value = "test value!"

        self.assertEqual(os.environ.get(name), None)
        os.environ[name] = value
        self.assertEqual(os.environ.get(name), value)

        with util.environment({}):
            self.assertEqual(os.environ.get(name), None)
        os.environ.pop(name)

        self.assertEqual(os.environ.get(name), None)

    def test_variables_set(self):
        name = "test_uuid_%s" % uuid4().hex
        value = "test value!"
        env = {name: value}

        self.assertEqual(os.environ.get(name), None)
        with util.environment(env):
            self.assertEqual(os.environ.get(name), value)
        self.assertEqual(os.environ.get(name), None)


if '__main__' == __name__:
    unittest.main()
