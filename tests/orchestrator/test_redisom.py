#!/usr/bin/env python

import unittest
import os
import flow.orchestrator.redisom as rom
from fakeredis import FakeRedis

class SimpleObj(rom.RedisObject):
    ascalar = rom.RedisScalar
    ahash = rom.RedisHash
    alist = rom.RedisList
    aset = rom.RedisSet
    a_method_arg = rom.RedisScalar

    def a_method(self, arg=None):
        self.a_method_arg = arg
        return arg


class RedisOmTest(unittest.TestCase):
    def setUp(self):
        import redis
        self.conn = redis.Redis()
        self.conn.flushall()


class TestRedisScalar(RedisOmTest):
    def test_value(self):
        x = rom.RedisScalar(self.conn, "x")
        self.assertEqual(None, x.value)
        x.value = "hello there"
        self.assertEqual("hello there", x.value)
        self.assertEqual("hello there", str(x))
        x.value = 32
        self.assertEqual(32, int(x))

    def test_setnx(self):
        x = rom.RedisScalar(self.conn, "x")
        self.assertTrue(x.setnx("hi"))
        self.assertEqual("hi", x.value)
        self.assertFalse(x.setnx("bye"))
        self.assertEqual("hi", x.value)

    def test_increment(self):
        x = rom.RedisScalar(self.conn, "x")
        x.value = 8
        self.assertEqual(16, x.increment(8))
        self.assertEqual(2, x.increment(-14))
        

class TestRedisList(RedisOmTest):
    def test_value(self):
        l = rom.RedisList(self.conn, "l")
        self.assertEqual([], l.value)
        native_list = ["one", "two"]
        l.value = native_list
        self.assertEqual(native_list, l.value)

        # make sure that reassignment clears the old values
        native_list = ["three", "four"]
        l.value = native_list
        self.assertEqual(native_list, l.value)

    def test_append(self):
        l = rom.RedisList(self.conn, "l")
        l.append("a")
        self.assertEqual(["a"], l.value)
        l.append("b")
        self.assertEqual(["a", "b"], l.value)

    def test_extend(self):
        l = rom.RedisList(self.conn, "l")
        l.extend(["a", "b"])
        self.assertEqual(["a", "b"], l.value)
        l.extend(["c", "d"])
        self.assertEqual(["a", "b", "c", "d"], l.value)

    def test_len(self):
        l = rom.RedisList(self.conn, "l")
        self.assertEqual(0, len(l))
        l.value = ["x", "y"]
        self.assertEqual(2, len(l))
        l.append("z")
        self.assertEqual(3, len(l))

    def test_getitem(self):
        l = rom.RedisList(self.conn, "l")
        self.assertRaises(IndexError, l.__getitem__, 0)
        self.assertRaises(TypeError, l.__getitem__, "cat")
        l.append("x")
        self.assertEqual("x", l[0])
        l.extend(["y", "z"])
        self.assertEqual("y", l[1])
        self.assertEqual("z", l[2])
        self.assertRaises(IndexError, l.__getitem__, 3)

    def test_setitem(self):
        l = rom.RedisList(self.conn, "l")
        l.value = ["one", "two"]
        l[0] = "three"
        l[1] = "four"
        self.assertEqual(["three", "four"], l.value)
        self.assertRaises(IndexError, l.__setitem__, 2, "five")

    def test_iterator(self):
        l = rom.RedisList(self.conn, "l")
        l.value = ["a", "b", "c"]
        seen = [x for x in l]
        self.assertEqual(["a", "b", "c"], l.value)
        

class TestRedisSet(RedisOmTest):
    def test_value(self):
        s = rom.RedisSet(self.conn, "s")
        self.assertEqual(set(), s.value)
        native_set = set(["one", "two", "three"])
        s.value = native_set
        self.assertEqual(native_set, s.value)

        native_set = set(["four"])
        s.value = native_set
        self.assertEqual(native_set, s.value)

    def test_add(self):
        s = rom.RedisSet(self.conn, "s")
        s.add("one")
        self.assertEqual(set(["one"]), s.value)
        s.add("one")
        self.assertEqual(set(["one"]), s.value)
        s.add("two")
        self.assertEqual(set(["one", "two"]), s.value)

    def test_update(self):
        s = rom.RedisSet(self.conn, "s")
        s.add("one")
        s.update(["one", "two"])
        self.assertEqual(set(["one", "two"]), s.value)
        s.update(["two", "three"])
        self.assertEqual(set(["one", "two", "three"]), s.value)

    def test_remove(self):
        s = rom.RedisSet(self.conn, "s")
        s.value = ["one", "two"]
        self.assertEqual(1, s.remove("two"))
        self.assertEqual(0, s.remove("two"))
        self.assertEqual(set(["one"]), s.value)
        self.assertEqual(1, s.remove("one"))
        self.assertEqual(set(), s.value)

    def test_len(self):
        s = rom.RedisSet(self.conn, "s")
        self.assertEqual(0, len(s))
        s.value = ["a", "b", "c"]
        self.assertEqual(3, len(s))
        s.remove("b")
        self.assertEqual(2, len(s))

    def test_iterator(self):
        s = rom.RedisSet(self.conn, "s")
        s.value = ["a", "b", "c"]
        self.assertEqual(["a", "b", "c"], sorted([x for x in s]))


class TestRedisHash(RedisOmTest):
    def test_value(self):
        h = rom.RedisHash(self.conn, "h")
        native_hash = {"hello": "world"}
        h.value = native_hash
        self.assertEqual(native_hash, h.value)

        native_hash = {"goodbye": "cruel world"}
        h.value = native_hash
        self.assertEqual(native_hash, h.value)

    def test_setitem(self):
        h = rom.RedisHash(self.conn, "h")
        h["x"] = "y"
        self.assertEqual({"x": "y"}, h.value)
        h["y"] = "z"
        self.assertEqual({"x": "y", "y": "z"}, h.value)
        h["y"] = "z"
        self.assertEqual({"x": "y", "y": "z"}, h.value)

    def test_getitem(self):
        h = rom.RedisHash(self.conn, "h")
        h.value = {"x": "y", "X": "Y"}
        self.assertEqual("y", h["x"])
        self.assertEqual("Y", h["X"])
        self.assertRaises(KeyError, h.__getitem__, "z")

    def test_delitem(self):
        h = rom.RedisHash(self.conn, "h")
        h.value = {"x": "y", "X": "Y"}
        self.assertRaises(KeyError, h.__delitem__, "z")
        del h["x"]
        self.assertEqual({"X": "Y"}, h.value)
        del h["X"]
        self.assertEqual({}, h.value)
    
    
    def test_len(self):
        h = rom.RedisHash(self.conn, "h")
        self.assertEqual(0, len(h))
        h["x"] = "y"
        self.assertEqual(1, len(h))
        h["y"] = "z"
        self.assertEqual(2, len(h))
        del h["y"]
        self.assertEqual(1, len(h))

    def test_keys_values(self):
        h = rom.RedisHash(self.conn, "h")
        native = dict((chr(x), str(x)) for x in xrange(ord('a'), ord('z')+1))
        h.value = native
        self.assertEqual(sorted(native.keys()), sorted(h.keys()))
        self.assertEqual(sorted(native.values()), sorted(h.values()))

    def test_update(self):
        h = rom.RedisHash(self.conn, "h")
        h.value = {"x": "y"}
        h.update({"x": "y", "y": "z"})
        self.assertEqual({"x": "y", "y": "z"}, h.value)
        h.update({"z": "a", "y": "z"})
        self.assertEqual({"x": "y", "y": "z", "z": "a"}, h.value)

    def test_iteritems(self):
        h = rom.RedisHash(self.conn, "h")
        native = dict((chr(x), str(x)) for x in xrange(ord('a'), ord('z')+1))
        h.value = native
        seen = dict((k, v) for k, v in h.iteritems())
        self.assertEqual(native, seen)


class TestRedisObject(RedisOmTest):
    def test_get_object(self):
        obj = SimpleObj.create(self.conn, "x", ascalar="hi")
        obj_ref = rom.get_object(self.conn, obj.key)
        self.assertEqual("x", obj.key)
        self.assertEqual("hi", obj.ascalar.value)

    def a_method_descriptor(self):
        obj = SimpleObj.create(self.conn, "x")
        expected = {"object_key": obj.key, "method_name": "a_method"}
        method_descriptor = obj.method_descriptor("a_method")
        self.assertEqual(expected, method_descriptor)
        self.assertFalse(None, obj.a_method_arg.value)
        rv = rom.invoke_instance_method(self.conn, method_descriptor, arg="yep")
        self.assertEqual("yep", rv)
        self.assertEqual("yep", obj.a_method_arg.value)

    def test_invalid_method_descriptor(self):
        obj = SimpleObj.create(self.conn, "x")
        self.assertRaises(AttributeError, obj.method_descriptor, "fake")
        method_descriptor = obj.method_descriptor("a_method")
        method_descriptor["method_name"] = "fake"
        self.assertRaises(AttributeError, rom.invoke_instance_method, 
                          self.conn, method_descriptor)

    def test_get_object_nexist(self):
        self.assertRaises(KeyError, SimpleObj.get, self.conn, "x")

    def test_create(self):
        obj = SimpleObj.create(self.conn, "x", ascalar=42)
        self.assertEqual(42, int(obj.ascalar))
        self.assertEqual("42", obj.ascalar.value)
        self.assertEqual({}, obj.ahash.value)
        self.assertEqual([], obj.alist.value)
        self.assertEqual(set(), obj.aset.value)

        obj = SimpleObj.create(self.conn, "y", ascalar=42,
                                ahash={'1': '2', '3': '4'})
        self.assertEqual(42, int(obj.ascalar))
        self.assertEqual('42', obj.ascalar.value)
        self.assertEqual({'1': '2', '3': '4'}, obj.ahash.value)
        self.assertEqual([], obj.alist.value)
        self.assertEqual(set(), obj.aset.value)

        obj = SimpleObj.create(self.conn, "y", ascalar=42,
                                ahash={'1': '2', '3': '4'},
                                alist=['5', '4', '3'])
        self.assertEqual(42, int(obj.ascalar))
        self.assertEqual('42', obj.ascalar.value)
        self.assertEqual({'1': '2', '3': '4'}, obj.ahash.value)
        self.assertEqual(['5', '4', '3'], obj.alist.value)
        self.assertEqual(set(), obj.aset.value)

        obj = SimpleObj.create(self.conn, "y", ascalar=42,
                                ahash={'1': '2', '3': '4'},
                                alist=['5', '4', '3'],
                                aset=['x', 'y', 'z'])
        self.assertEqual(42, int(obj.ascalar))
        self.assertEqual('42', obj.ascalar.value)
        self.assertEqual({'1': '2', '3': '4'}, obj.ahash.value)
        self.assertEqual(['5', '4', '3'], obj.alist.value)
        self.assertEqual(set(['x', 'y', 'z']), obj.aset.value)

        
    def test_create_invalid_prop(self):
        self.assertRaises(AttributeError, SimpleObj.create, self.conn, "x",
                          badprop="bad")

    def test_subkey(self):
        obj = SimpleObj.create(self.conn, "/x")
        self.assertEqual("/x/y/z", obj.subkey("y", "z"))
        self.assertEqual("/x/1/2", obj.subkey(1, 2))


if __name__ == "__main__":
    unittest.main()
