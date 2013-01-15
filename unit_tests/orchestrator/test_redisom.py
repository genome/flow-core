#!/usr/bin/env python

import flow.orchestrator.redisom as rom

import os
import unittest
from redistest import RedisTest

class SimpleObj(rom.Object):
    """A simple class with one of each type of property to test rom.Object"""

    ascalar = rom.Property(rom.Scalar)
    atimestamp = rom.Property(rom.Timestamp)
    ahash = rom.Property(rom.Hash)
    alist = rom.Property(rom.List)
    aset = rom.Property(rom.Set)
    a_method_arg = rom.Property(rom.Scalar)

    def a_method(self, arg=None):
        self.a_method_arg = arg
        return arg

class OtherObj(rom.Object):
    """Used to test what happens getting an object of the wrong type"""
    pass


class TestBase(unittest.TestCase):
    def setUp(self):
        self.conn = RedisTest()
        self.conn.flushall()


class TestEncoders(TestBase):
    def test_json_enc_dec(self):
        self.assertEqual('null', rom.json_enc(None))
        self.assertEqual(None, rom.json_dec('null'))
        self.assertEqual(None, rom.json_dec(None))

        val = {"one": [2, 3, 4], "five": 6}
        enc = rom.json_enc(val)
        self.assertTrue(isinstance(enc, basestring))
        self.assertEqual(val, rom.json_dec(enc))

class TestProperty(TestBase):
    def test_property_class_validity(self):
        self.assertRaises(TypeError, rom.Property, basestring)
        self.assertRaises(TypeError, rom.Property, rom.Property)
        self.assertRaises(TypeError, rom.Property, int)
        self.assertRaises(TypeError, rom.Property, "rom.Scalar")

        # There is no assertNotRaises, so we just do these to make sure they
        # don't throw.
        rom.Property(rom.Hash)
        rom.Property(rom.List)
        rom.Property(rom.Scalar)
        rom.Property(rom.Set)

class TestScalar(TestBase):
    def test_value(self):
        x = rom.Scalar(self.conn, "x")
        self.assertEqual(None, x.value)
        x.value = "hello there"
        self.assertEqual("hello there", x.value)
        self.assertEqual("hello there", str(x))
        x.value = 32
        self.assertEqual(32, int(x))
        self.assertEqual("'32'", repr(x))

    def test_setnx(self):
        x = rom.Scalar(self.conn, "x")
        self.assertTrue(x.setnx("hi"))
        self.assertEqual("hi", x.value)
        self.assertFalse(x.setnx("bye"))
        self.assertEqual("hi", x.value)

    def test_increment(self):
        x = rom.Scalar(self.conn, "x")
        x.value = 8
        self.assertEqual(16, x.increment(8))
        self.assertEqual(2, x.increment(-14))


class TestTimestamp(TestBase):
    def test_timestamp(self):
        ts = rom.Timestamp(self.conn, "ts")
        self.assertEqual(None, ts.value)

        first = ts.setnx()
        self.assertFalse(first is False)
        self.assertTrue(float(first) >= 0)
        self.assertEqual(first, ts.value)

        second = ts.set()
        self.assertTrue(float(second) >= float(first))
        self.assertEqual(second, ts.value)

        self.assertFalse(ts.setnx())
        self.assertEqual(second, ts.value)

    def test_delete(self):
        ts = rom.Timestamp(self.conn, "ts")
        val = ts.setnx()
        self.assertTrue(float(val) > 0)
        ts.delete()
        self.assertEqual(None, ts.value)
        val2 = ts.setnx()
        self.assertTrue(float(val2) >= float(val))


class TestList(TestBase):
    def test_value(self):
        l = rom.List(self.conn, "l")
        self.assertEqual([], l.value)
        native_list = ["one", "two"]
        l.value = native_list
        self.assertEqual(native_list, l.value)

        # make sure that reassignment clears the old values
        native_list = ["three", "four"]
        l.value = native_list
        self.assertEqual(native_list, l.value)
        self.assertEqual(repr(native_list), repr(l))

    def test_append(self):
        l = rom.List(self.conn, "l")
        size = l.append("a")
        self.assertEqual(["a"], l.value)
        self.assertEqual(1, size)

        size = l.append("b")
        self.assertEqual(["a", "b"], l.value)
        self.assertEqual(2, size)

    def test_extend(self):
        l = rom.List(self.conn, "l")
        l.extend(["a", "b"])
        self.assertEqual(["a", "b"], l.value)
        l.extend(["c", "d"])
        self.assertEqual(["a", "b", "c", "d"], l.value)

    def test_len(self):
        l = rom.List(self.conn, "l")
        self.assertEqual(0, len(l))
        l.value = ["x", "y"]
        self.assertEqual(2, len(l))
        l.append("z")
        self.assertEqual(3, len(l))

    def test_getitem(self):
        l = rom.List(self.conn, "l")
        self.assertRaises(IndexError, l.__getitem__, 0)
        self.assertRaises(TypeError, l.__getitem__, "cat")
        l.append("x")
        self.assertEqual("x", l[0])
        l.extend(["y", "z"])
        self.assertEqual("y", l[1])
        self.assertEqual("z", l[2])
        self.assertRaises(IndexError, l.__getitem__, 3)

    def test_setitem(self):
        l = rom.List(self.conn, "l")
        l.value = ["one", "two"]
        l[0] = "three"
        l[1] = "four"
        self.assertEqual(["three", "four"], l.value)
        self.assertRaises(IndexError, l.__setitem__, 2, "five")

    def test_iterator(self):
        l = rom.List(self.conn, "l")
        l.value = ["a", "b", "c"]
        seen = [x for x in l]
        self.assertEqual(["a", "b", "c"], l.value)


class TestSet(TestBase):
    def test_value(self):
        s = rom.Set(self.conn, "s")
        self.assertEqual(set(), s.value)
        native_set = set(["one", "two", "three"])
        s.value = native_set
        self.assertEqual(native_set, s.value)

        native_set = set(["four"])
        s.value = native_set
        self.assertEqual(native_set, s.value)
        self.assertEqual(repr(native_set), repr(s))

    def test_add(self):
        s = rom.Set(self.conn, "s")
        s.add("one")
        self.assertEqual(set(["one"]), s.value)
        s.add("one")
        self.assertEqual(set(["one"]), s.value)
        s.add("two")
        self.assertEqual(set(["one", "two"]), s.value)

    def test_update(self):
        s = rom.Set(self.conn, "s")
        s.add("one")
        s.update(["one", "two"])
        self.assertEqual(set(["one", "two"]), s.value)
        s.update(["two", "three"])
        self.assertEqual(set(["one", "two", "three"]), s.value)

    def test_remove(self):
        s = rom.Set(self.conn, "s")
        s.value = ["one", "two"]
        self.assertEqual((True, 1), s.remove("two"))
        self.assertEqual((False, 1), s.remove("two"))
        self.assertEqual(set(["one"]), s.value)
        self.assertEqual((True, 0), s.remove("one"))
        self.assertEqual(set(), s.value)

    def test_len(self):
        s = rom.Set(self.conn, "s")
        self.assertEqual(0, len(s))
        s.value = ["a", "b", "c"]
        self.assertEqual(3, len(s))
        s.remove("b")
        self.assertEqual(2, len(s))

    def test_iterator(self):
        s = rom.Set(self.conn, "s")
        s.value = ["a", "b", "c"]
        self.assertEqual(["a", "b", "c"], sorted([x for x in s]))


class TestHash(TestBase):
    def test_value(self):
        h = rom.Hash(self.conn, "h")
        native_hash = {"hello": "world"}
        h.value = native_hash
        self.assertEqual(native_hash, h.value)

        native_hash = {"goodbye": "cruel world"}
        h.value = native_hash
        self.assertEqual(native_hash, h.value)
        self.assertEqual(repr(native_hash), repr(h))

    def test_set_empty(self):
        h = rom.Hash(self.conn, "h")
        h.value = {"a": "b"}
        self.assertEqual(1, len(h))
        h.value = {}
        self.assertEqual(0, len(h))
        self.assertEqual({}, h.value)

    def test_setitem(self):
        h = rom.Hash(self.conn, "h")
        h["x"] = "y"
        self.assertEqual({"x": "y"}, h.value)
        h["y"] = "z"
        self.assertEqual({"x": "y", "y": "z"}, h.value)
        h["y"] = "z"
        self.assertEqual({"x": "y", "y": "z"}, h.value)

    def test_getitem(self):
        h = rom.Hash(self.conn, "h")
        h.value = {"x": "y", "X": "Y"}
        self.assertEqual("y", h["x"])
        self.assertEqual("Y", h["X"])
        self.assertRaises(KeyError, h.__getitem__, "z")

    def test_delitem(self):
        h = rom.Hash(self.conn, "h")
        h.value = {"x": "y", "X": "Y"}
        self.assertRaises(KeyError, h.__delitem__, "z")
        del h["x"]
        self.assertEqual({"X": "Y"}, h.value)
        del h["X"]
        self.assertEqual({}, h.value)


    def test_len(self):
        h = rom.Hash(self.conn, "h")
        self.assertEqual(0, len(h))
        h["x"] = "y"
        self.assertEqual(1, len(h))
        h["y"] = "z"
        self.assertEqual(2, len(h))
        del h["y"]
        self.assertEqual(1, len(h))

    def test_keys_values(self):
        h = rom.Hash(self.conn, "h")
        native = dict((chr(x), str(x)) for x in xrange(ord('a'), ord('z')+1))
        h.value = native
        self.assertEqual(sorted(native.keys()), sorted(h.keys()))
        self.assertEqual(sorted(native.values()), sorted(h.values()))

    def test_update(self):
        h = rom.Hash(self.conn, "h")
        h.value = {"x": "y"}
        h.update({"x": "y", "y": "z"})
        self.assertEqual({"x": "y", "y": "z"}, h.value)
        h.update({"z": "a", "y": "z"})
        self.assertEqual({"x": "y", "y": "z", "z": "a"}, h.value)

        self.assertEqual(None, h.update({}))
        self.assertEqual({"x": "y", "y": "z", "z": "a"}, h.value)

    def test_iteritems(self):
        h = rom.Hash(self.conn, "h")
        native = dict((chr(x), str(x)) for x in xrange(ord('a'), ord('z')+1))
        h.value = native
        seen = dict((k, v) for k, v in h.iteritems())
        self.assertEqual(native, seen)

    def test_json_encoding(self):
        h = rom.Hash(self.conn, "h",
                     value_encoder=rom.json_enc,
                     value_decoder=rom.json_dec)

        native = {"a": ["b", "c"], "d": {"e": "f"}, "g": "h", "num": 7}
        h.value = native

        # test .value property
        self.assertEqual(native, h.value)

        # test __getitem__
        for k in native:
            self.assertEqual(native[k], h[k])

        # test .values()
        self.assertEqual(native.values(), h.values())
        partial_keys = ("a", "num", "g")
        native_partial = dict((k, native[k]) for k in partial_keys)
        self.assertEqual(native_partial.values(), h.values(partial_keys))

        # test .iteritems()
        seen = dict((k, v) for k, v in h.iteritems())
        self.assertEqual(native, seen)

        # test .update()
        upd = {"one": { "two": ["three", "four", "five"] } }
        h.update(upd)
        self.assertEqual(upd["one"], h["one"])


class TestObject(TestBase):
    def test_keygen(self):
        # If you are here because you just changed the key generation policy
        # to not include module/class name, then feel free to remove this
        # test.
        obj = SimpleObj.create(self.conn)
        components = obj.key.split("/")
        self.assertEqual(4, len(components))
        self.assertEqual('', components[0])
        self.assertEqual(obj.__module__, components[1])
        self.assertEqual(obj.__class__.__name__, components[2])


    def test_get_object_not_found(self):
        self.assertRaises(KeyError, rom.get_object, self.conn, "badkey")
        self.assertRaises(KeyError, SimpleObj.get, self.conn, "badkey")

    def test_get_object(self):
        obj = SimpleObj.create(self.conn, "x", ascalar="hi")

        obj_ref = rom.get_object(self.conn, obj.key)
        self.assertEqual("x", obj_ref.key)
        self.assertEqual("hi", obj_ref.ascalar.value)

        obj_ref = SimpleObj.get(self.conn, "x")
        self.assertEqual("x", obj.key)
        self.assertEqual("hi", obj.ascalar.value)

    def test_get_object_wrong_type(self):
        obj = SimpleObj.create(self.conn, "x", ascalar="hi")
        self.assertRaises(TypeError, OtherObj.get, self.conn, "x")

        obj = OtherObj.create(self.conn, "x")
        self.assertRaises(TypeError, SimpleObj.get, self.conn, "x")

    def test_method_descriptor(self):
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

    def test_delete_property(self):
        obj = SimpleObj.create(self.conn, "/x")

        obj.ascalar = "six"
        key = obj.ascalar.key
        self.assertEqual("six", self.conn.get(key))
        del obj.ascalar
        self.assertFalse(hasattr(obj, "ascalar"))
        self.assertEqual(None, self.conn.get(key))

        obj.ahash = {"a": "b"}
        key = obj.ahash.key
        self.assertEqual({"a": "b"}, self.conn.hgetall(key))
        del obj.ahash
        self.assertFalse(hasattr(obj, "ahash"))
        self.assertEqual(None, self.conn.get(key))

        obj.aset = set(["x", "y"])
        key = obj.aset.key
        self.assertEqual(set(["x", "y"]), self.conn.smembers(key))
        del obj.aset
        self.assertFalse(hasattr(obj, "aset"))
        # redis quirk: smembers returns set([]) when fetching an empty set
        self.assertEqual(set([]), self.conn.smembers(key))

        obj.alist = ["a", "b", "c"]
        key = obj.alist.key
        self.assertEqual(["a", "b", "c"], self.conn.lrange(key, 0, -1))
        del obj.alist
        self.assertFalse(hasattr(obj, "alist"))
        # redis quirk: smembers returns list([]) when fetching an empty list
        self.assertEqual(0, self.conn.llen(key))



if __name__ == "__main__":
    unittest.main()
