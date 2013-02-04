#!/usr/bin/env python

import flow.orchestrator.redisom as rom

import os
import redis
import unittest

_good_script = "return {1, 2, '3'}"

_bad_script = "i am bad"

_return_keys_script = "return KEYS"

_return_args_script = "return ARGV"

class ScriptObj(rom.Object):
    good = rom.Script(script_body=_good_script)
    bad = rom.Script(script_body=_bad_script)
    keys = rom.Script(script_body=_return_keys_script)
    args = rom.Script(script_body=_return_args_script)


class TestBase(unittest.TestCase):
    def setUp(self):
        redis_host = os.environ['FLOW_TEST_REDIS_URL']
        self.conn = redis.Redis(redis_host)


class TestRedisOm(TestBase):
    def setUp(self):
        TestBase.setUp(self)
        self.obj = ScriptObj(self.conn, key="x")

    def test_good(self):
        self.assertEqual([1, 2, '3'], self.obj.good())

    def test_bad(self):
        self.assertRaises(redis.exceptions.ResponseError, self.obj.bad)

    def test_keys_and_args(self):
        self.assertEqual(["a", "b", "c"], self.obj.keys(
            keys=["a", "b", "c"], args=[1, 2, 3]))

        self.assertEqual(['1', '2', '3'], self.obj.args(
            keys=["a", "b", "c"], args=['1', '2', '3']))


if __name__ == "__main__":
    unittest.main()