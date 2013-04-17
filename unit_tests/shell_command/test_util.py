import unittest
try:
    from unittest import mock
except:
    import mock

import os
from uuid import uuid4
from flow.shell_command import util


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
