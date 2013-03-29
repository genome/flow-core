import unittest
try:
    from unittest import mock
except:
    import mock

import os
from uuid import uuid4
from flow.command_runner import util


class EnvironmentContextManagerTest(unittest.TestCase):
    def test_no_contamination(self):
        name = "test_uuid_%s" % uuid4().hex
        value = "test value!"

        self.assertEqual(os.environ.get(name), None)
        os.environ[name] = value
        self.assertEqual(os.environ.get(name), value)

        with util.environment([{}]):
            self.assertEqual(os.environ.get(name), None)
        os.environ.pop(name)

        self.assertEqual(os.environ.get(name), None)

    def test_variables_set(self):
        name = "test_uuid_%s" % uuid4().hex
        value = "test value!"
        env = {name: value}

        self.assertEqual(os.environ.get(name), None)
        with util.environment([env]):
            self.assertEqual(os.environ.get(name), value)
        self.assertEqual(os.environ.get(name), None)

    def test_override(self):
        name = "old_test_uuid_%s" % uuid4().hex
        old_value = "old test value!"
        new_value = "new test value!"

        old_env = {name: old_value}
        new_env = {name: new_value}

        self.assertEqual(os.environ.get(name), None)
        with util.environment([old_env, new_env]):
            self.assertEqual(os.environ.get(name), new_value)

        self.assertEqual(os.environ.get(name), None)

    def test_exception(self):
        old_env = dict(os.environ.data)

        try:
            with util.environment([{}]):
                raise RuntimeError()
        except:
            pass

        self.assertEqual(old_env, os.environ.data)
        print os.environ.data


class JoinPathIfRelTest(unittest.TestCase):
    def test_is_relative(self):
        parts = ['/root', 'middle', 'relative/guy.txt']
        self.assertEqual(util.join_path_if_rel(*parts),
                '/root/middle/relative/guy.txt')

    def test_is_absolute(self):
        parts = ['/root', 'middle', '/absolute/guy.txt']
        self.assertEqual(util.join_path_if_rel(*parts),
                '/absolute/guy.txt')

    def test_error_in_final_path_component(self):
        parts = ['/root', 'middle', None]
        self.assertRaises(RuntimeError, util.join_path_if_rel, *parts)

    def test_error_in_other_path_components(self):
        parts = ['/root', None, 'relative/guy.txt']
        self.assertRaises(RuntimeError, util.join_path_if_rel, *parts)

    def test_not_enough_arguments(self):
        self.assertRaises(RuntimeError, util.join_path_if_rel)


if '__main__' == __name__:
    unittest.main()
