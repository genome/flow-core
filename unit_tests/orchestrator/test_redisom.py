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
    aref = rom.Reference('SimpleObj')
    astrongref = rom.Reference('SimpleObj', weak=False)

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
        self.assertEqual('null', rom.json_enc(None, None))
        self.assertEqual(None, rom.json_dec(None, 'null'))
        self.assertEqual(None, rom.json_dec(None, None))

        val = {"one": [2, 3, 4], "five": 6}
        enc = rom.json_enc(None, val)
        self.assertTrue(isinstance(enc, basestring))
        self.assertEqual(val, rom.json_dec(None, enc))

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

class TestReference(TestBase):
    def setUp(self):
        TestBase.setUp(self)
        self.x = SimpleObj.create(connection=self.conn, key='/x')
        self.y = SimpleObj.create(connection=self.conn, key='/y')

    def tearDown(self):
        del self.x
        del self.y

    def test_access_reference_before_set(self):
        self.assertRaises(RuntimeError, getattr, self.x, 'aref')

    def test_chained_references(self):
        z = SimpleObj.create(connection=self.conn, key='/z')
        z.ascalar = 'z-scalar'
        self.x.aref = self.y
        self.assertEqual(self.x.aref.key, self.y.key)
        self.y.aref = z
        self.assertEqual(self.y.aref.key, z.key)

        self.assertEqual(self.x.aref.aref.key, z.key)
        self.assertEqual(self.x.aref.aref.ascalar.value, 'z-scalar')

    def test_redis_backed_references(self):
        self.x.aref = self.y
        self.assertEqual(self.x.aref.key, self.y.key)

        another_x = rom.get_object(self.conn, self.x.key)
        self.assertEqual(another_x.aref.key, self.y.key)

    def test_self_reference(self):
        self.x.aref = self.x
        self.x.ascalar = 'x-scalar'
        self.assertEqual(self.x.aref.key, self.x.key)
        self.assertEqual(self.x.aref.aref.key, self.x.key)
        self.assertEqual(self.x.aref.aref.aref.key, self.x.key)

        self.assertEqual(self.x.aref.ascalar.value, 'x-scalar')
        self.assertEqual(self.x.aref.aref.ascalar.value, 'x-scalar')
        self.assertEqual(self.x.aref.aref.aref.ascalar.value, 'x-scalar')

    def test_delete_reference(self):
        self.x.aref = self.y
        self.assertEqual(self.x.aref.key, self.y.key)
        del self.x.aref
        self.assertRaises(RuntimeError, getattr, self.x, 'aref')

        self.x.aref = self.x
        self.assertEqual(self.x.aref.key, self.x.key)
        del self.x.aref
        self.assertRaises(RuntimeError, getattr, self.x, 'aref')

    def test_change_reference(self):
        self.x.aref = self.y
        self.x.ascalar = 'x-scalar'
        self.y.ascalar = 'y-scalar'
        self.assertEqual(self.x.aref.key, self.y.key)
        self.assertEqual(self.x.aref.ascalar.value, 'y-scalar')

        self.x.aref = self.x
        self.assertEqual(self.x.aref.key, self.x.key)
        self.assertEqual(self.x.aref.ascalar.value, 'x-scalar')

    def test_delete_object_with_weak_references(self):
        self.y.ascalar = 'y-scalar'
        self.x.aref = self.y
        self.assertEqual(self.x.aref.key, self.y.key)
        self.assertEqual(self.x.aref.ascalar.value, 'y-scalar')

        self.x.delete()
        self.assertFalse(self.conn.exists(self.x.key))
        self.assertTrue(self.conn.exists(self.y.key))
        self.assertTrue(self.conn.exists(self.y.ascalar.key))

    def test_delete_object_with_strong_references(self):
        self.y.ascalar = 'y-scalar'
        self.x.astrongref = self.y
        self.assertEqual(self.x.astrongref.key, self.y.key)
        self.assertEqual(self.x.astrongref.ascalar.value, 'y-scalar')

        self.x.delete()
        self.assertFalse(self.conn.exists(self.x.key))
        self.assertFalse(self.conn.exists(self.y.key))
        self.assertFalse(self.conn.exists(self.y.ascalar.key))

    def test_try_add_with_non_object(self):
        self.assertRaises(TypeError, setattr, self.x, 'aref', 'bad')

    def test_try_to_specify_a_non_object_class(self):
        def make_class():
            class Bad(rom.Object):
                bad_ref = rom.Reference(rom.Scalar)
        self.assertRaises(TypeError, make_class)

    def test_specifying_a_class(self):
        class TestObj(rom.Object):
            aref = rom.Reference(SimpleObj)
        i = TestObj.create(connection=self.conn, key='/i')
        i.aref = self.x
        self.assertEqual(i.aref.key, self.x.key)


class TestScalar(TestBase):
    def test_value(self):
        x = rom.Scalar(connection=self.conn, key="x")
        self.assertRaises(KeyError, getattr, x, 'value')
        x.value = "hello there"
        self.assertEqual("hello there", x.value)
        self.assertEqual("hello there", str(x))
        x.value = 32
        self.assertEqual(32, int(x))
        self.assertEqual('32', str(x))
        self.assertRaises(TypeError, rom.Scalar)

    def test_setnx(self):
        x = rom.Scalar(connection=self.conn, key="x")
        self.assertTrue(x.setnx("hi"))
        self.assertEqual("hi", x.value)
        self.assertFalse(x.setnx("bye"))
        self.assertEqual("hi", x.value)

    def test_incr(self):
        x = rom.Scalar(connection=self.conn, key="x")
        x.value = 8
        self.assertEqual(16, x.incr(8))
        self.assertEqual(2, x.incr(-14))


class TestTimestamp(TestBase):
    def test_timestamp(self):
        ts = rom.Timestamp(connection=self.conn, key="ts")
        self.assertRaises(KeyError, getattr, ts, "value")

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
        ts = rom.Timestamp(connection=self.conn, key="ts")
        val = ts.setnx()
        self.assertTrue(float(val) > 0)
        ts.delete()
        self.assertRaises(KeyError, getattr, ts, "value")
        val2 = ts.setnx()
        self.assertTrue(float(val2) >= float(val))

    def test_setter(self):
        ts = rom.Timestamp(connection=self.conn, key="ts")
        def tester():
            ts.value = 'something'
        self.assertRaises(AttributeError, tester)



class TestList(TestBase):
    def test_value(self):
        l = rom.List(connection=self.conn, key="l")
        self.assertEqual([], l.value)
        native_list = ["one", "two"]
        l.value = native_list
        self.assertEqual(native_list, l.value)

        # make sure that reassignment clears the old values
        native_list = ["three", "four"]
        l.value = native_list
        self.assertEqual(native_list, l.value)
        self.assertEqual(str(native_list), str(l))

    def test_append(self):
        l = rom.List(connection=self.conn, key="l")
        size = l.append("a")
        self.assertEqual(["a"], l.value)
        self.assertEqual(1, size)

        size = l.append("b")
        self.assertEqual(["a", "b"], l.value)
        self.assertEqual(2, size)

    def test_extend(self):
        l = rom.List(connection=self.conn, key="l")
        l.extend(["a", "b"])
        self.assertEqual(["a", "b"], l.value)
        l.extend(["c", "d"])
        self.assertEqual(["a", "b", "c", "d"], l.value)

    def test_len(self):
        l = rom.List(connection=self.conn, key="l")
        self.assertEqual(0, len(l))
        l.value = ["x", "y"]
        self.assertEqual(2, len(l))
        l.append("z")
        self.assertEqual(3, len(l))

    def test_getitem(self):
        l = rom.List(connection=self.conn, key="l")
        self.assertRaises(IndexError, l.__getitem__, 0)
        self.assertRaises(TypeError, l.__getitem__, "cat")
        l.append("x")
        self.assertEqual("x", l[0])
        l.extend(["y", "z"])
        self.assertEqual("y", l[1])
        self.assertEqual("z", l[2])
        self.assertRaises(IndexError, l.__getitem__, 3)

    def test_setitem(self):
        l = rom.List(connection=self.conn, key="l")
        l.value = ["one", "two"]
        l[0] = "three"
        l[1] = "four"
        self.assertEqual(["three", "four"], l.value)
        self.assertRaises(IndexError, l.__setitem__, 2, "five")

    def test_iterator(self):
        l = rom.List(connection=self.conn, key="l")
        l.value = ["a", "b", "c"]
        seen = [x for x in l]
        self.assertEqual(["a", "b", "c"], l.value)


class TestSet(TestBase):
    def test_value(self):
        s = rom.Set(connection=self.conn, key="s")
        self.assertEqual(set(), s.value)
        native_set = set(["one", "two", "three"])
        s.value = native_set
        self.assertEqual(native_set, s.value)

        native_set = set(["four"])
        s.value = native_set
        self.assertEqual(native_set, s.value)
        self.assertEqual(str(native_set), str(s))

    def test_add(self):
        s = rom.Set(connection=self.conn, key="s")
        s.add("one")
        self.assertEqual(set(["one"]), s.value)
        s.add("one")
        self.assertEqual(set(["one"]), s.value)
        s.add("two")
        self.assertEqual(set(["one", "two"]), s.value)

    def test_update(self):
        s = rom.Set(connection=self.conn, key="s")
        s.add("one")
        s.update(["one", "two"])
        self.assertEqual(set(["one", "two"]), s.value)
        s.update(["two", "three"])
        self.assertEqual(set(["one", "two", "three"]), s.value)

    def test_discard(self):
        s = rom.Set(connection=self.conn, key="s")
        s.value = ["one", "two"]
        self.assertEqual((True, 1), s.discard("two"))
        self.assertEqual((False, 1), s.discard("two"))
        self.assertEqual(set(["one"]), s.value)
        self.assertEqual((True, 0), s.discard("one"))
        self.assertEqual(set(), s.value)

    def test_remove(self):
        s = rom.Set(connection=self.conn, key="s")
        s.value = ["one", "two"]
        self.assertEqual((True, 1), s.remove("two"))
        self.assertRaises(KeyError, s.remove, "two")
        self.assertEqual(set(["one"]), s.value)
        self.assertEqual((True, 0), s.remove("one"))
        self.assertEqual(set(), s.value)

    def test_len(self):
        s = rom.Set(connection=self.conn, key="s")
        self.assertEqual(0, len(s))
        s.value = ["a", "b", "c"]
        self.assertEqual(3, len(s))
        s.remove("b")
        self.assertEqual(2, len(s))

    def test_iterator(self):
        s = rom.Set(connection=self.conn, key="s")
        s.value = ["a", "b", "c"]
        self.assertEqual(["a", "b", "c"], sorted([x for x in s]))


class TestHash(TestBase):
    def setUp(self):
        TestBase.setUp(self)
        self.h = rom.Hash(connection=self.conn, key="h")

    def test_value(self):
        native_hash = {"hello": "world"}
        self.h.value = native_hash
        self.assertEqual(native_hash, self.h.value)

        native_hash = {"goodbye": "cruel world"}
        self.h.value = native_hash
        self.assertEqual(native_hash, self.h.value)
        self.assertEqual(str(native_hash), str(self.h))

    def test_set_empty(self):
        self.h.value = {"a": "b"}
        self.assertEqual(1, len(self.h))
        self.h.value = {}
        self.assertEqual(0, len(self.h))
        self.assertEqual({}, self.h.value)

    def test_setitem(self):
        self.h["x"] = "y"
        self.assertEqual({"x": "y"}, self.h.value)
        self.h["y"] = "z"
        self.assertEqual({"x": "y", "y": "z"}, self.h.value)
        self.h["y"] = "z"
        self.assertEqual({"x": "y", "y": "z"}, self.h.value)

        he = rom.Hash(connection=self.conn, key='he', value_encoder=rom.json_enc)
        he['x'] = 'y'
        self.assertEqual(he['x'], '"y"')

    def test_getitem(self):
        self.h.value = {"x": "y", "X": "Y"}
        self.assertEqual("y", self.h["x"])
        self.assertEqual("Y", self.h["X"])
        self.assertRaises(KeyError, self.h.__getitem__, "z")

    def test_delitem(self):
        self.h.value = {"x": "y", "X": "Y"}
        self.assertRaises(KeyError, self.h.__delitem__, "z")
        del self.h["x"]
        self.assertEqual({"X": "Y"}, self.h.value)
        del self.h["X"]
        self.assertEqual({}, self.h.value)

    def test_len(self):
        self.assertEqual(0, len(self.h))
        self.h["x"] = "y"
        self.assertEqual(1, len(self.h))
        self.h["y"] = "z"
        self.assertEqual(2, len(self.h))
        del self.h["y"]
        self.assertEqual(1, len(self.h))

    def test_keys_values(self):
        native = dict((chr(x), str(x)) for x in xrange(ord('a'), ord('z')+1))
        self.h.value = native
        self.assertEqual(sorted(native.keys()), sorted(self.h.keys()))
        self.assertEqual(sorted(native.values()), sorted(self.h.values()))

    def test_incrby(self):
        self.h.value = {'a':1}
        self.assertEqual('1', str(self.h['a']))
        self.assertEqual('3', str(self.h.incrby('a', 2)))
        self.assertEqual('3', str(self.h['a']))

    def test_get(self):
        self.h.value = {'a':1}
        self.assertEqual('1', str(self.h['a']))
        self.assertEqual('1', str(self.h.get('a')))
        self.assertEqual('default', self.h.get('b', default='default'))
        self.assertEqual(None, self.h.get('b'))

    def test_update(self):
        self.h.value = {"x": "y"}
        self.h.update({"x": "y", "y": "z"})
        self.assertEqual({"x": "y", "y": "z"}, self.h.value)
        self.h.update({"z": "a", "y": "z"})
        self.assertEqual({"x": "y", "y": "z", "z": "a"}, self.h.value)

        self.assertEqual(None, self.h.update({}))
        self.assertEqual({"x": "y", "y": "z", "z": "a"}, self.h.value)

    def test_iteritems(self):
        native = dict((chr(x), str(x)) for x in xrange(ord('a'), ord('z')+1))
        self.h.value = native
        seen = dict((k, v) for k, v in self.h.iteritems())
        self.assertEqual(native, seen)

    def test_json_encoding(self):
        h = rom.Hash(connection=self.conn, key="h",
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
        obj = SimpleObj.create(connection=self.conn)
        components = obj.key.split("/")
        self.assertEqual(4, len(components))
        self.assertEqual('', components[0])
        self.assertEqual(obj.__module__, components[1])
        self.assertEqual(obj.__class__.__name__, components[2])

    def test_access_non_property_or_reference(self):
        obj = SimpleObj.create(connection=self.conn, key="x")
        def access():
            obj.not_a_member
        self.assertRaises(AttributeError, access)

        obj.something_else = 'a member'
        self.assertEqual(obj.something_else, 'a member')

    def test_get_object_not_found(self):
        self.assertRaises(KeyError, rom.get_object, connection=self.conn,
                key="badkey")
        self.assertRaises(KeyError, SimpleObj.get, connection=self.conn,
                key="badkey")

    def test_get_object(self):
        obj = SimpleObj.create(connection=self.conn, key="x", ascalar="hi")

        obj_ref = rom.get_object(connection=self.conn, key=obj.key)
        self.assertEqual("x", obj_ref.key)
        self.assertEqual("hi", obj_ref.ascalar.value)

        obj_ref = SimpleObj.get(connection=self.conn, key="x")
        self.assertEqual("x", obj.key)
        self.assertEqual("hi", obj.ascalar.value)

        self.assertRaises(TypeError, SimpleObj.get)

    def test_get_object_wrong_type(self):
        obj = SimpleObj.create(connection=self.conn, key="x", ascalar="hi")
        self.assertRaises(TypeError, OtherObj.get, connection=self.conn,
                key="x")

        obj = OtherObj.create(connection=self.conn, key="x")
        self.assertRaises(TypeError, SimpleObj.get, connection=self.conn,
                key="x")

    def test_method_descriptor(self):
        obj = SimpleObj.create(connection=self.conn, key="x")
        expected = {"object_key": obj.key, "method_name": "a_method"}
        method_descriptor = obj.method_descriptor("a_method")
        self.assertEqual(expected, method_descriptor)
        self.assertRaises(KeyError, getattr, obj.a_method_arg, 'value')
        rv = rom.invoke_instance_method(self.conn, method_descriptor,
                arg="yep")
        self.assertEqual("yep", rv)
        self.assertEqual("yep", obj.a_method_arg.value)

    def test_invalid_method_descriptor(self):
        obj = SimpleObj.create(connection=self.conn, key="x")
        self.assertRaises(AttributeError, obj.method_descriptor, "fake")
        method_descriptor = obj.method_descriptor("a_method")
        method_descriptor["method_name"] = "fake"
        self.assertRaises(AttributeError, rom.invoke_instance_method,
                          self.conn, method_descriptor)

    def test_get_object_nexist(self):
        self.assertRaises(KeyError, SimpleObj.get, connection=self.conn,
                key="x")

    def test_bad_create(self):
        self.assertRaises(TypeError, SimpleObj.create)

    def test_create(self):
        obj = SimpleObj.create(connection=self.conn, key="x", ascalar=42)
        self.assertEqual(42, int(obj.ascalar))
        self.assertEqual("42", obj.ascalar.value)
        self.assertEqual({}, obj.ahash.value)
        self.assertEqual([], obj.alist.value)
        self.assertEqual(set(), obj.aset.value)

        obj = SimpleObj.create(connection=self.conn, key="y", ascalar=42,
                                ahash={'1': '2', '3': '4'})
        self.assertEqual(42, int(obj.ascalar))
        self.assertEqual('42', obj.ascalar.value)
        self.assertEqual({'1': '2', '3': '4'}, obj.ahash.value)
        self.assertEqual([], obj.alist.value)
        self.assertEqual(set(), obj.aset.value)

        obj = SimpleObj.create(connection=self.conn, key="z", ascalar=42,
                                ahash={'1': '2', '3': '4'},
                                alist=['5', '4', '3'])
        self.assertEqual(42, int(obj.ascalar))
        self.assertEqual('42', obj.ascalar.value)
        self.assertEqual({'1': '2', '3': '4'}, obj.ahash.value)
        self.assertEqual(['5', '4', '3'], obj.alist.value)
        self.assertEqual(set(), obj.aset.value)

        obj = SimpleObj.create(connection=self.conn, key="zz", ascalar=42,
                                ahash={'1': '2', '3': '4'},
                                alist=['5', '4', '3'],
                                aset=['x', 'y', 'z'])
        self.assertEqual(42, int(obj.ascalar))
        self.assertEqual('42', obj.ascalar.value)
        self.assertEqual({'1': '2', '3': '4'}, obj.ahash.value)
        self.assertEqual(['5', '4', '3'], obj.alist.value)
        self.assertEqual(set(['x', 'y', 'z']), obj.aset.value)


    def test_create_invalid_prop(self):
        self.assertRaises(AttributeError, SimpleObj.create,
                connection=self.conn, key="x", badprop="bad")

    def test_subkey(self):
        obj = SimpleObj.create(connection=self.conn, key="/x")
        self.assertEqual("/x/y/z", obj.subkey("y", "z"))
        self.assertEqual("/x/1/2", obj.subkey(1, 2))

    def test_delete_property(self):
        obj = SimpleObj.create(connection=self.conn, key="/x")

        obj.ascalar = "six"
        key = obj.ascalar.key
        self.assertEqual("six", self.conn.get(key))
        del obj.ascalar
        self.assertEqual(None, self.conn.get(key))

        obj.ahash = {"a": "b"}
        key = obj.ahash.key
        self.assertEqual({"a": "b"}, self.conn.hgetall(key))
        del obj.ahash
        self.assertEqual(None, self.conn.get(key))

        obj.aset = set(["x", "y"])
        key = obj.aset.key
        self.assertEqual(set(["x", "y"]), self.conn.smembers(key))
        del obj.aset
        # redis quirk: smembers returns set([]) when fetching an empty set
        self.assertEqual(set([]), self.conn.smembers(key))

        obj.alist = ["a", "b", "c"]
        key = obj.alist.key
        self.assertEqual(["a", "b", "c"], self.conn.lrange(key, 0, -1))
        del obj.alist
        # redis quirk: smembers returns list([]) when fetching an empty list
        self.assertEqual(0, self.conn.llen(key))

    def test_delete_object(self):
        obj = SimpleObj.create(connection=self.conn, key="/x")

        obj.ascalar = "six"
        key = obj.ascalar.key
        self.assertEqual("six", self.conn.get(key))

        obj.ahash = {"a": "b"}
        key = obj.ahash.key
        self.assertEqual({"a": "b"}, self.conn.hgetall(key))

        obj.aset = set(["x", "y"])
        key = obj.aset.key
        self.assertEqual(set(["x", "y"]), self.conn.smembers(key))

        obj.alist = ["a", "b", "c"]
        key = obj.alist.key
        self.assertEqual(["a", "b", "c"], self.conn.lrange(key, 0, -1))

        obj.delete()

        self.assertEqual(None, self.conn.get(key))
        self.assertEqual(None, self.conn.get(key))
        # redis quirk: smembers returns set([]) when fetching an empty set
        self.assertEqual(set([]), self.conn.smembers(key))
        # redis quirk: smembers returns list([]) when fetching an empty list
        self.assertEqual(0, self.conn.llen(key))


if __name__ == "__main__":
    unittest.main()
