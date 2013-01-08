#!/usr/bin/env python

import unittest
import os
import flow.orchestrator.redisom as rom
from fakeredis import FakeRedis

class TestObject(rom.RedisObject):
    ascalar = rom.RedisScalar
    ahash = rom.RedisHash
    alist = rom.RedisList
    aset = rom.RedisSet

class TestRedisObject(unittest.TestCase):
    def setUp(self):
        self.conn = FakeRedis()

    def test_get_object_nexist(self):
        self.assertRaises(KeyError, TestObject.get, self.conn, "x")

    def test_create(self):
        obj = TestObject.create(self.conn, "x", ascalar=42)
        self.assertEqual(42, int(obj.ascalar))
        self.assertEqual('42', obj.ascalar.value)
        self.assertEqual({}, obj.ahash.value)
        self.assertEqual([], obj.alist.value)
        self.assertEqual(set(), obj.aset.value)

        obj = TestObject.create(self.conn, "y", ascalar=42,
                                ahash={'1': '2', '3': '4'})
        self.assertEqual(42, int(obj.ascalar))
        self.assertEqual('42', obj.ascalar.value)
        self.assertEqual({'1': '2', '3': '4'}, obj.ahash.value)
        self.assertEqual([], obj.alist.value)
        self.assertEqual(set(), obj.aset.value)

        obj = TestObject.create(self.conn, "y", ascalar=42,
                                ahash={'1': '2', '3': '4'},
                                alist=['5', '4', '3'])
        self.assertEqual(42, int(obj.ascalar))
        self.assertEqual('42', obj.ascalar.value)
        self.assertEqual({'1': '2', '3': '4'}, obj.ahash.value)
        self.assertEqual(['5', '4', '3'], obj.alist.value)
        self.assertEqual(set(), obj.aset.value)

        obj = TestObject.create(self.conn, "y", ascalar=42,
                                ahash={'1': '2', '3': '4'},
                                alist=['5', '4', '3'],
                                aset=['x', 'y', 'z'])
        self.assertEqual(42, int(obj.ascalar))
        self.assertEqual('42', obj.ascalar.value)
        self.assertEqual({'1': '2', '3': '4'}, obj.ahash.value)
        self.assertEqual(['5', '4', '3'], obj.alist.value)
        self.assertEqual(set(['x', 'y', 'z']), obj.aset.value)

        
    def test_create_invalid_prop(self):
        self.assertRaises(AttributeError, TestObject.create, self.conn, "x",
                          badprop="bad")

    def test_subkey(self):
        obj = TestObject.create(self.conn, "/x")
        self.assertEqual("/x/y/z", obj.subkey("y", "z"))
        self.assertEqual("/x/1/2", obj.subkey(1, 2))

if __name__ == "__main__":
    unittest.main()
