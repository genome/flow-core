#!/usr/bin/env python

import flow.redisom as rom

import os
import unittest
import redis
from mock import Mock, patch

from test_helpers.redistest import RedisTest

_good_script = "return {1, 2, '3'}"

_bad_script = "i am bad"

_return_keys_script = "return KEYS"

_return_args_script = "return ARGV"

class ScriptObj(rom.Object):
    good = rom.Script(script_body=_good_script)
    bad = rom.Script(script_body=_bad_script)
    keys = rom.Script(script_body=_return_keys_script)
    args = rom.Script(script_body=_return_args_script)


class SimpleObj(rom.Object):
    """A simple class with one of each type of property to test rom.Object"""

    ascalar = rom.Property(rom.String)
    atimestamp = rom.Property(rom.Timestamp)
    ahash = rom.Property(rom.Hash)
    alist = rom.Property(rom.List)
    aset = rom.Property(rom.Set)
    a_method_arg = rom.Property(rom.String)

    def a_method(self, arg=None):
        self.a_method_arg = arg
        return arg


class TestRedisOm(RedisTest):
    def setUp(self):
        RedisTest.setUp(self)
        self.obj = ScriptObj.create(self.conn, key="x")

    def test_good(self):
        self.assertEqual([1, 2, '3'], self.obj.good())

    def test_bad(self):
        self.assertRaises(redis.exceptions.ResponseError, self.obj.bad)

    def test_keys_and_args(self):
        self.assertEqual(["a", "b", "c"], self.obj.keys(
            keys=["a", "b", "c"], args=[1, 2, 3]))

        self.assertEqual(['1', '2', '3'], self.obj.args(
            keys=["a", "b", "c"], args=['1', '2', '3']))


class TestCopyScript(RedisTest):
    def test_direct_primitives(self):
        self.conn.set("scalar1", "hello world")
        rv = rom.copy_key(self.conn, "scalar1", "scalar2")
        self.assertEqual("hello world", self.conn.get("scalar1"))
        self.assertEqual("hello world", self.conn.get("scalar2"))

        h = {"a": "b", "c": "d"}
        self.conn.hmset("h1", h)
        rv = rom.copy_key(self.conn, "h1", "h2")
        self.assertEqual(h, self.conn.hgetall("h1"))
        self.assertEqual(h, self.conn.hgetall("h2"))

        s = set(["cow", "chicken", "orange"])
        self.conn.sadd("s1", *s)
        rom.copy_key(self.conn, "s1", "s2")
        self.assertEqual(s, self.conn.smembers("s1"))
        self.assertEqual(s, self.conn.smembers("s2"))

        l = ["apple", "blueberry", "eagle"]
        self.conn.rpush("l1", *l)
        rom.copy_key(self.conn, "l1", "l2")
        self.assertEqual(l, self.conn.lrange("l1", 0, -1))
        self.assertEqual(l, self.conn.lrange("l2", 0, -1))

    def test_string(self):
        s1 = rom.String(self.conn, "x")
        s1.value = "hey there"
        s2 = s1.copy("y")

        self.assertEqual("hey there", str(s1))
        self.assertEqual("hey there", str(s2))

        s1.value = "how's it going?"

        self.assertEqual("how's it going?", str(s1))
        self.assertEqual("hey there", str(s2))

        s2.value = "pretty good"

        self.assertEqual("how's it going?", str(s1))
        self.assertEqual("pretty good", str(s2))


    def test_int(self):
        i1 = rom.Int(self.conn, "x")
        i1.value = 42

        i2 = i1.copy("y")

        self.assertEqual(42, i1.value)
        self.assertEqual(42, i2.value)

        i1.incr(8)

        self.assertEqual(50, i1.value)
        self.assertEqual(42, i2.value)

        i2.incr(3)

        self.assertEqual(50, i1.value)
        self.assertEqual(45, i2.value)


class RomValueCopyBase(object):
    rom_type = None
    init_args = {}
    three_values = []

    def test_copy(self):
        obj1 = self.rom_type(connection=self.conn, key="v1", **self.init_args)
        obj1.value = self.three_values[0]
        obj2 = obj1.copy("v2")

        self.assertEqual("v2", obj2.key)

        self.assertEqual(self.three_values[0], obj1.value)
        self.assertEqual(self.three_values[0], obj2.value)

        obj1.value = self.three_values[1]
        self.assertEqual(self.three_values[1], obj1.value)
        self.assertEqual(self.three_values[0], obj2.value)

        obj2.value = self.three_values[2]
        self.assertEqual(self.three_values[1], obj1.value)
        self.assertEqual(self.three_values[2], obj2.value)


class TestStringCopy(RedisTest, RomValueCopyBase):
    rom_type = rom.String
    three_values = ["x", "y", "z"]


class TestIntCopy(RedisTest, RomValueCopyBase):
    rom_type = rom.Int
    three_values = [10, 100, 1000]


class TestFloatCopy(RedisTest, RomValueCopyBase):
    rom_type = rom.Float
    three_values = [1.5, 2.5, 3.5]


class TestSetCopy(RedisTest, RomValueCopyBase):
    rom_type = rom.Set
    three_values = [set(["a", "b"]), set(["x", "y"]), set()]


class TestListCopy(RedisTest, RomValueCopyBase):
    rom_type = rom.List
    three_values = [["a", "b"], ["c", "d"], ["e", "f"]]


class TestJsonListCopy(RedisTest, RomValueCopyBase):
    rom_type = rom.List
    init_args = {"value_encoder": rom.json_enc, "value_decoder": rom.json_dec}
    three_values = [[1, "two"], [{"three": 4}, None], [[1,2,3,4]]]


class TestHashCopy(RedisTest, RomValueCopyBase):
    rom_type = rom.Hash
    three_values = [{"a": "b"}, {"c": "d"}, {"e": "f"}]


class TestJsonHashCopy(RedisTest, RomValueCopyBase):
    rom_type = rom.Hash
    init_args = {"value_encoder": rom.json_enc, "value_decoder": rom.json_dec}
    three_values = [{"a": [1,2,3]}, {"c": "d"}, {"e": {"f": [1, None, {}]}}]


class TestTimestampCopy(RedisTest):
    def test_copy(self):
        # Patch "now" on the timestamp object (which returns the current time
        # in floating point seconds) to return a custom sequence
        with patch.object(rom.Timestamp, "now") as mock_now:
            mock_now.__get__ = Mock(return_value='1.1')
            ts1 = rom.Timestamp(connection=self.conn, key="ts1")

            ts1.setnx()
            ts2 = ts1.copy("ts2")
            self.assertEqual("ts2", ts2.key)

            self.assertEqual(1.1, float(ts1))
            self.assertEqual(1.1, float(ts2))


class TestCopyObject(RedisTest):
    def test_copy(self):
        args = {"ascalar": "Scalar value",
                "ahash": {"a": "b", "c": "d"},
                "alist": ["x", "y", "z"],
                "aset": set(["one", "two", "three"]),
                }

        obj1 = SimpleObj.create(connection=self.conn, key="key1", **args)

        args['atimestamp'] = obj1.atimestamp.setnx()

        initial_keys = self.conn.keys()

        for arg, expected in args.iteritems():
            value = getattr(obj1, arg).value
            self.assertEqual(expected, value)

        obj2 = obj1.copy("key2")
        self.assertEqual("key2", obj2.key)

        for arg, expected in args.iteritems():
            value = getattr(obj2, arg).value
            self.assertEqual(expected, value)

        keys = self.conn.keys()
        self.assertEqual(len(initial_keys)*2, len(keys))
        keys1 = [x.split("/", 1)[1] for x in keys if x.startswith("key1/")]
        keys2 = [x.split("/", 1)[1] for x in keys if x.startswith("key2/")]

        self.assertItemsEqual(keys1, keys2)

class TestDeleteFunctionality(RedisTest):
    def test_value_delete(self):
        v = rom.Value(connection=self.conn, key='test-key')
        v.value = 'test-value'

        self.assertTrue(self.conn.exists(v.key))
        v.delete()
        self.assertFalse(self.conn.exists(v.key))

    def test_object_delete(self):
        obj = SimpleObj.create(connection=self.conn, key='test-obj')
        obj.ahash = {'some hash key':5}
        print "Redis has %d keys after creating the object." % len(self.conn.keys())

        obj.delete()
        print "Redis has %d keys after DELETING the object." % len(self.conn.keys())
        print self.conn.keys()
        self.assertEqual(0, len(self.conn.keys()))


if __name__ == "__main__":
    unittest.main()
