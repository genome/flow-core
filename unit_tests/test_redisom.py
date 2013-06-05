#!/usr/bin/env python

from flow.redisom import NotInRedisError
from test_helpers.fakeredistest import FakeRedisTest

import flow.redisom as rom
import os
import unittest


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


class OtherObj(rom.Object):
    """Used to test what happens getting an object of the wrong type"""
    pass


class TestEncoders(FakeRedisTest):
    def test_json_enc_dec(self):
        self.assertEqual('null', rom.json_enc(None))
        self.assertEqual(None, rom.json_dec('null'))
        self.assertEqual(None, rom.json_dec(None))

        val = {"one": [2, 3, 4], "five": 6}
        enc = rom.json_enc(val)
        self.assertTrue(isinstance(enc, basestring))
        self.assertEqual(val, rom.json_dec(enc))


class TestProperty(FakeRedisTest):
    def test_property_class_validity(self):
        self.assertRaises(TypeError, rom.Property, basestring)
        self.assertRaises(TypeError, rom.Property, rom.Property)
        self.assertRaises(TypeError, rom.Property, int)
        self.assertRaises(TypeError, rom.Property, "rom.Int")

        # There is no assertNotRaises, so we just do these to make sure they
        # don't throw.
        rom.Property(rom.Hash)
        rom.Property(rom.List)
        rom.Property(rom.Int)
        rom.Property(rom.Set)


class TestValue(FakeRedisTest):
    def setUp(self):
        FakeRedisTest.setUp(self)
        self.x = rom.Value(connection=self.conn, key='/x')

    def test_init(self):
        self.assertRaises(TypeError, rom.Value)

    def test_value(self):
        self.assertRaises(KeyError, getattr, self.x, 'value')
        self.x.value = "hello there"
        self.assertEqual("hello there", self.x.value)
        self.assertEqual("hello there", str(self.x))
        self.x.value = 32
        self.assertEqual(32, int(self.x))
        self.assertEqual('32', str(self.x))
        self.assertEqual(32, float(self.x))

    def test_setnx(self):
        self.assertTrue(self.x.setnx("hi"))
        self.assertEqual("hi", self.x.value)
        self.assertFalse(self.x.setnx("bye"))
        self.assertEqual("hi", self.x.value)


class TestInt(FakeRedisTest):
    def setUp(self):
        FakeRedisTest.setUp(self)
        self.x = rom.Int(connection=self.conn, key='/x')
        self.c = rom.Int(connection=self.conn, key='/c', cacheable=True)

    def test_value(self):
        self.assertRaises(ValueError, setattr, self.x, 'value', 'string')
        self.x.value = 1234
        self.assertEqual(1234, self.x.value)
        self.x.value = 1.234
        self.assertEqual(1, self.x.value)

    def test_incr(self):
        self.x.value = 8
        self.assertEqual(9, self.x.incr())
        self.assertEqual(9, self.x.value)

        self.assertEqual(11, self.x.incr(2))
        self.assertEqual(11, self.x.value)

    def test_decr(self):
        self.x.value = 8
        self.assertEqual(7, self.x.decr())
        self.assertEqual(7, self.x.value)

        self.assertEqual(5, self.x.decr(2))
        self.assertEqual(5, self.x.value)

    def test_cachable(self):
        c = rom.Int(connection=self.conn, key='/c', cacheable=True)
        self.conn.set(c.key, 42)
        self.assertEqual(42, c.value)

        self.conn.set(c.key, 7)
        self.assertEqual(42, c.value)

    def test_immutable(self):
        i = rom.Int(connection=self.conn, key='/i', immutable=True)
        i._set_raw_value(7)
        self.assertEqual(7, i.value)
        self.assertRaises(ValueError, setattr, i, 'value', 42)
        self.assertEqual(7, i.value)


class TestFloat(FakeRedisTest):
    def setUp(self):
        FakeRedisTest.setUp(self)
        self.x = rom.Float(connection=self.conn, key='/x')
        self.c = rom.Float(connection=self.conn, key='/c', cacheable=True)

    def test_value(self):
        self.assertRaises(ValueError, setattr, self.x, 'value', 'string')
        self.x.value = 1234
        self.assertEqual(1234, self.x.value)
        self.x.value = 1.234
        self.assertEqual(1.234, self.x.value)

    def test_incr(self):
        self.x.value = 8
        self.assertEqual(9, self.x.incr())
        self.assertEqual(9, self.x.value)

        self.assertEqual(11, self.x.incr(2))
        self.assertEqual(11, self.x.value)

    def test_cachable(self):
        self.conn.set(self.c.key, 42.3)
        self.assertEqual(42.3, self.c.value)

        self.conn.set(self.c.key, 7.8)
        self.assertEqual(42.3, self.c.value)

    def test_immutable(self):
        f = rom.Float(connection=self.conn, key='/f', immutable=True)
        f._set_raw_value(2.1)
        self.assertEqual(2.1, f.value)
        self.assertRaises(ValueError, setattr, f, 'value', 12.3)
        self.assertEqual(2.1, f.value)


class TestString(FakeRedisTest):
    def setUp(self):
        FakeRedisTest.setUp(self)
        self.x = rom.String(connection=self.conn, key='/x')
        self.c = rom.String(connection=self.conn, key='/c', cacheable=True)

    def test_value(self):
        self.x.value = 1234
        self.assertEqual("1234", self.x.value)

    def test_cachable(self):
        self.conn.set(self.c.key, "hi")
        self.assertEqual("hi", self.c.value)

        self.conn.set(self.c.key, "bye")
        self.assertEqual("hi", self.c.value)

    def test_immutable(self):
        s = rom.String(connection=self.conn, key='/s', immutable=True)
        s._set_raw_value('hi')
        self.assertEqual('hi', s.value)
        self.assertRaises(ValueError, setattr, s, 'value', 'bye')
        self.assertEqual('hi', s.value)


class TestTimestamp(FakeRedisTest):
    def test_uninitialized(self):
        ts = rom.Timestamp(self.conn, "ts")
        self.assertRaises(NotInRedisError, getattr, ts, "value")

    def test_setnx_succeed(self):
        ts = rom.Timestamp(self.conn, "ts")
        rv = ts.setnx(1.23)
        self.assertEqual(1.23, rv)
        self.assertEqual(1.23, ts.value)

    def test_setnx_already_set(self):
        ts = rom.Timestamp(self.conn, "ts")
        ts.setnx(1.23)

        rv = ts.setnx(4.56)
        self.assertEqual(False, rv)
        self.assertEqual(1.23, ts.value)

    def test_delete(self):
        ts = rom.Timestamp(connection=self.conn, key="ts")
        val = ts.setnx()
        self.assertTrue(float(val) > 0)
        ts.delete()
        self.assertRaises(KeyError, getattr, ts, "value")
        val2 = ts.setnx()
        self.assertTrue(float(val2) >= float(val))

    def test_setter(self):
        ts = rom.Timestamp(self.conn, "ts")
        self.assertRaises(ValueError, setattr, ts, 'value', 'DUMMY')

    def test_cachable(self):
        ts = rom.Timestamp(connection=self.conn, key="ts")
        ts.setnx()
        value = ts.value

        self.conn.set(ts.key, str(0.0))
        self.assertEqual(str(0.0), self.conn.get(ts.key))
        self.assertEqual(value, ts.value)

    def test_immutable(self):
        ts = rom.Timestamp(connection=self.conn, key="ts")
        value = ts.setnx()
        self.assertEqual(value, ts.value)
        self.assertRaises(ValueError, setattr, ts, 'value', 12.3)
        self.assertEqual(value, ts.value)


class TestList(FakeRedisTest):
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

    def test_value_encode_decode(self):
        l = rom.List(connection=self.conn, key='l',
                value_encoder=rom.json_enc, value_decoder=rom.json_dec)

        e0 = {'arbitrary': 3}
        e1 = ['data', 7.2]

        l.extend([e0, e1])

        r0 = l[0]
        self.assertEqual(e0, r0)

        r1 = l[1]
        self.assertEqual(e1, r1)


class TestSet(FakeRedisTest):
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

    def test_add_return_size(self):
        s = rom.Set(connection=self.conn, key="s")
        rv, size = s.add_return_size("one")
        self.assertEqual(1, rv)
        self.assertEqual(1, size)
        self.assertItemsEqual(["one"], s.value)

        rv, size = s.add_return_size("one")
        self.assertEqual(0, rv)
        self.assertEqual(1, size)
        self.assertItemsEqual(["one"], s.value)

        rv, size = s.add_return_size("two")
        self.assertEqual(1, rv)
        self.assertEqual(2, size)
        self.assertItemsEqual(["one", "two"], s.value)


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

    def test_cachable(self):
        s = rom.Set(connection=self.conn, key="s", cacheable=True)
        s.value = set(['a', 'b', 'c'])
        value = s.value

        self.conn.sadd(s.key, 'd')
        self.assertEqual(set(['d']).union(value), self.conn.smembers(s.key))
        self.assertEqual(value, s.value)


class TestHash(FakeRedisTest):
    def setUp(self):
        FakeRedisTest.setUp(self)
        self.h = rom.Hash(connection=self.conn, key="h")

    def test_contains(self):
        self.h["x"] = "y"
        self.assertIn("x", self.h)
        self.assertNotIn("y", self.h)

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

    def test_getitem_encoded_null(self):
        he = rom.Hash(connection=self.conn, key="he", value_encoder=rom.json_enc,
                value_decoder=rom.json_dec)
        he.value = {"x": None}
        self.assertEqual(None, he["x"])

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


class TestObject(FakeRedisTest):
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

    def test_access_non_property_or(self):
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
        self.assertRaises(TypeError, rom.get_object)

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

    def test_class_not_loaded_in_specified_module(self):
        class_info = 'unit_tests:LoadableObj'
        self.conn.set('/y', class_info)
        self.conn.set('/y/ascalar', '1234')

        self.assertRaises(ImportError, rom.get_object, self.conn, '/y')
        self.assertRaises(ImportError, rom.Object.get_class, class_info)


if __name__ == "__main__":
    unittest.main()
