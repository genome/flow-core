#!/usr/bin/env python

# pylint: disable=W0212

from uuid import uuid4
import functools
import hashlib
import redis
import json
import re

KEY_DELIM = '/'

def json_enc(obj):
    return json.dumps(obj)

def json_dec(text):
    if text is None:
        return None
    else:
        return json.loads(text)


class RomIndexError(IndexError):
    pass

class NotInRedisError(KeyError):
    pass


class Script(object):
    def __init__(self, script_body=None):
        self.script_body = script_body
        self.script_hash = hashlib.sha1(script_body).hexdigest()

    def __call__(self, connection=None, keys=[], args=[]):
        if connection is None:
            raise TypeError("You must specify a connection")

        num_keys = len(keys)
        keys_and_args = keys + args
        try:
            return connection.evalsha(self.script_hash,
                    num_keys, *keys_and_args)
        except redis.exceptions.NoScriptError:
            return connection.eval(self.script_body, num_keys, *keys_and_args)


class Property(object):
    def __init__(self, cls, **kwargs):
        if not issubclass(cls, Value):
            raise TypeError("Unknown redisom class %s" % str(cls))

        self.cls = cls
        self.kwargs = kwargs

class Reference(object):
    def __init__(self, class_or_class_info, weak=True, **kwargs):
        if isinstance(class_or_class_info, basestring):
            self.class_info = class_or_class_info
        else:
            cls = class_or_class_info
            if not issubclass(cls, Object):
                raise TypeError("References must be to subclasses of Object "
                    "and (%s) is not!" % cls.__name__)
            self.class_info = cls._info

        self.weak = weak
        self.kwargs = kwargs


class Value(object):
    def __init__(self, connection=None, key=None):
        if connection is None or key is None:
            raise TypeError("You must specify a connection and a key")
        self.connection = connection
        self.key = key

    @classmethod
    def create(cls, *args, **kwargs):
        return cls(*args, **kwargs)

    def setnx(self, value):
        return self.connection.setnx(self.key, self._encode(value))

    def delete(self):
        return self.connection.delete(self.key)

    def _encode(self, value):
        return value

    def _decode(self, value):
        return value

    def _value_getter(self):
        result = self.connection.get(self.key)
        if result is None:
            raise NotInRedisError("Redis has no data for key (%s)." %
                    (self.key))
        return self._decode(result)

    def _value_setter(self, new_value):
        return self.connection.set(self.key, self._encode(new_value))

    def __int__(self):
        return int(self.value)

    def __float__(self):
        return float(self.value)

    def __str__(self):
        return str(self.value)

    value = property(_value_getter, _value_setter)

class Timestamp(Value):
    def _value_setter(self, new_value):
        raise AttributeError("You cannot set the .value of a Timestamp.")

    value = property(Value._value_getter, _value_setter)

    @property
    def now(self):
        sec, usec = self.connection.time()
        return "%d.%d" % (sec, usec)

    def set(self):
        now = self.now
        self.connection.set(self.key, now)
        return now

    def setnx(self, value=None):
        if value is None:
            value = self.now
        if self.connection.setnx(self.key, value):
            return value
        return False

class Int(Value):
    def incr(self, *args, **kwargs):
        return self.connection.incr(self.key, *args, **kwargs)

    def decr(self, *args, **kwargs):
        return self.connection.decr(self.key, *args, **kwargs)

    def _encode(self, value):
        return int(value)

    def _decode(self, value):
        return int(value)


class Float(Int):
    def _encode(self, value):
        return float(value)

    def _decode(self, value):
        return float(value)


class String(Value):
    def _encode(self, value):
        return str(value)

class List(Value):
    def __getitem__(self, idx):
        try:
            idx = int(idx)
        except:
            raise TypeError("list indices must be integers, not str")

        result = self.connection.lindex(self.key, idx)
        # NOTE We accept the constraint that list values cannot be None here
        if result is None:
            raise RomIndexError("list index out of range "
                    "(key=%s, index=%d)" % (self.key, idx))
        return result

    def __setitem__(self, idx, val):
        try:
            return self.connection.lset(self.key, idx, val)
        except redis.ResponseError:
            raise RomIndexError ("list index out of range "
                    "(key=%s, size=%d, index=%d)" % (self.key, len(self), idx))

    def __len__(self):
        return self.connection.llen(self.key)

    def _value_getter(self):
        return self.connection.lrange(self.key, 0, -1)

    def _value_setter(self, val):
        self.connection.delete(self.key)
        if val:
            return self.connection.rpush(self.key, *val)

    value = property(_value_getter, _value_setter)

    def append(self, val):
        return self.extend([val])

    def extend(self, vals):
        # Something in the redis module doesn't work well with
        # generators, so we need an actual list
        vals = list(vals)
        if vals:
            ret = self.connection.rpush(self.key, *vals)
            return ret


class Set(Value):
    def _value_getter(self):
        return self.connection.smembers(self.key)

    def _value_setter(self, val):
        pipe = self.connection.pipeline()
        pipe.delete(self.key)
        if val:
            pipe.sadd(self.key, *val)
        size, value = pipe.execute()
        return value

    value = property(_value_getter, _value_setter)

    def add(self, val):
        return self.connection.sadd(self.key, val)

    def remove(self, val):
        removed, size = self.discard(val)
        if not removed:
            raise KeyError("Set (%s) doesn't contain value %s" %
                    (self.key, val))
        return removed, size

    def discard(self, val):
        pipe = self.connection.pipeline()
        pipe.srem(self.key, val)
        pipe.scard(self.key)
        removed, size = pipe.execute()
        return removed, size

    def update(self, vals):
        return self.connection.sadd(self.key, *vals)

    def __iter__(self):
        return self.value.__iter__()

    def __len__(self):
        return self.connection.scard(self.key)


class Hash(Value):
    def __init__(self, value_encoder=None, value_decoder=None, **kwargs):
        Value.__init__(self, **kwargs)
        self._value_encoder = value_encoder
        self._value_decoder = value_decoder

    def _encode_dict(self, d):
        if self._value_encoder is None:
            return d
        else:
            encoder = self._value_encoder
            return dict((k, encoder(v)) for k, v in d.iteritems())

    def _decode_dict(self, d):
        if self._value_encoder is None:
            return d
        else:
            decoder = self._value_decoder
            return dict((k, decoder(v)) for k, v in d.iteritems())

    def _encode_value(self, v):
        if self._value_encoder is None:
            return v
        else:
            return self._value_encoder(v)

    def _decode_value(self, v):
        if self._value_decoder is None:
            return v
        else:
            return self._value_decoder(v)

    def _decode_values(self, values):
        if self._value_decoder is None:
            return values
        else:
            decoder = self._value_decoder
            return [decoder(v) for v in values]

    def incrby(self, key, n):
        return self.connection.hincrby(self.key, key, n)

    def _value_getter(self):
        raw = self.connection.hgetall(self.key)
        return self._decode_dict(raw)

    def _value_setter(self, d):
        if d:
            pipe = self.connection.pipeline()
            pipe.delete(self.key)
            pipe.hmset(self.key, self._encode_dict(d))
            pipe.execute()
        else:
            return self.connection.delete(self.key)

    value = property(_value_getter, _value_setter)

    def __setitem__(self, hkey, val):
        return self.connection.hset(self.key, hkey, self._encode_value(val))

    def __getitem__(self, hkey):
        result = self.get(hkey)
        if result is None:
            raise KeyError("Hash (%s) has no key '%s'" % (self.key, hkey))
        return self._decode_value(result)

    def get(self, hkey, default=None):
        result = self.connection.hget(self.key, hkey)
        if result is None:
            return default
        return result

    def __delitem__(self, hkey):
        pipe = self.connection.pipeline()
        pipe.hexists(self.key, hkey)
        pipe.hdel(self.key, hkey)
        exists, rv = pipe.execute()
        if not exists:
            raise KeyError("Hash (%s) has no key '%s'" % (self.key, hkey))
        return rv

    def __len__(self):
        return self.connection.hlen(self.key)

    def keys(self):
        return self.connection.hkeys(self.key)

    def values(self, keys=None):
        if keys is None:
            raw = self.connection.hvals(self.key)
        else:
            raw = self.connection.hmget(self.key, keys)
        return self._decode_values(raw)

    def update(self, other):
        if not other:
            return None
        return self.connection.hmset(self.key, self._encode_dict(other))

    def iteritems(self):
        return self.value.iteritems()


def _make_key(*args):
    return KEY_DELIM.join(map(str, args))


class ObjectMeta(type):
    _class_registry = {}
    _class_info_re = re.compile(r"^[A-Za-z0-9_.]+:[A-Za-z0-9_]+$")
    def __new__(mcs, class_name, bases, class_dict):
        cls = type.__new__(mcs, class_name, bases, class_dict)

        members = {}
        for base in bases:
            members.update(base.__dict__)
        members.update(class_dict)

        rom_dict = {
                "_rom_scripts":Script,
                "_rom_properties":Property,
                "_rom_references":Reference,
                }

        for hidden_variable, hidden_class in rom_dict.items():
            setattr(cls, hidden_variable, {})
            this_hv = getattr(cls, hidden_variable)

            for base in bases:
                this_hv.update(getattr(base, hidden_variable, {}))

            for name, value in members.iteritems():
                if isinstance(value, hidden_class):
                    this_hv[name] = value
                    delattr(cls, name)

        class_info = "%s:%s" % (cls.__module__, cls.__name__)
        mcs._class_registry[class_info] = cls
        cls._info = class_info
        return cls

    def get_class(cls, class_info):
        try:
            return cls._class_registry[class_info]
        except KeyError:
            if cls._class_info_re.match(class_info) is None:
                raise TypeError("Improperly formatted class_info (%s) must "
                        "be formatted as <module>:<class_name>" % class_info)

            class_module, class_name = class_info.split(':')
            __import__(class_module)
            try:
                return cls._class_registry[class_info]
            except KeyError:
                raise ImportError("Expected to register class %s when loading "
                        "module %s but failed to." % (class_name, class_module))


class Object(object):
    __metaclass__ = ObjectMeta

    def __init__(self, connection=None, key=None):
        if connection is None or key is None:
            raise TypeError("You must specify a connection and a key")
        self.__dict__.update({
            "key": key,
            "_cache": {},
            "_class_info": String(connection=connection, key=key),
            "connection": connection,
        })

        for name, script in self._rom_scripts.iteritems():
            self.__dict__[name] = functools.partial(script, self.connection)


    def __getattr__(self, name):
        # fallback for when lookup in __dict__ fails
        try:
            return self._cache[name]
        except KeyError:
            try:
                propdef = self._rom_properties[name]
            except KeyError:
                try:
                    propdef = self._rom_references[name]
                except:
                    raise AttributeError("No such property or relation '%s'"
                            " on class %s" % (name, self.__class__.__name__))
                else:
                    key = self.connection.get(self.subkey(name))
                    if key is None:
                        raise RuntimeError("Reference has not been set yet.")
                    cls = Object.get_class(propdef.class_info)
                    prop = cls(connection=self.connection, key=key,
                            **propdef.kwargs)
                    self._cache[name] = prop
            else:
                cls = propdef.cls
                prop = cls.create(connection=self.connection,
                        key=self.subkey(name), **propdef.kwargs)
                self._cache[name] = prop

            return prop

    def __setattr__(self, name, value):
        # is always called on attribute setting
        if name in self._rom_properties:
            getattr(self, name).value = value
        elif name in self._rom_references:
            if isinstance(value, basestring):
                key = value
                class_info = self.connection.get(key)
                if class_info is None:
                    raise NotInRedisError("Redis has no data for key (%s)." %
                            key)
                cls = Object.get_class(class_info)

                expected_class_info = self._rom_references[name].class_info
                if cls._info != expected_class_info:
                    raise TypeError("Attempted to assign to %s but %s "
                            "doesn't match the expected class info (%s)" %
                            (name, cls._info, expected_class_info))
                obj = cls(self.connection, key)
            else:
                expected_class_info = self._rom_references[name].class_info
                if not isinstance(value, Object):
                    raise TypeError("Attempted to assign to %s but %s isn't an"
                            "instance of Object")
                else:
                    if value._info != expected_class_info:
                        raise TypeError("Attempted to assign to %s but %s "
                                "doesn't match the expected class info (%s)" %
                                (name, value._info, expected_class_info))
                key = value.key
                obj = value

            self.connection.set(self.subkey(name), key)
            self._cache[name] = obj
        else:
            object.__setattr__(self, name, value)

    def __delattr__(self, name):
        if name in self._rom_properties:
            getattr(self, name).delete()
        elif name in self._rom_references:
            self.connection.delete(self.subkey(name))
            # Is it a bug that this is unused?
#            ref = self._rom_references[name]

        if name in self._cache:
            del self._cache[name]

    def exists(self):
        try:
            class_info = self._class_info.value
        except NotInRedisError:
            return False

        if class_info != self._info:
            raise TypeError("Classinfo for %s isn't correct, found '%s' "
                    "expected '%s'" % (self.key, class_info, self._info))
        return True

    def _on_create(self):
        pass

    @classmethod
    def create(cls, connection=None, key=None, **kwargs):
        if key is None:
            key = _make_key("/" + cls.__module__, cls.__name__,
                            uuid4().hex)

        obj = cls(connection=connection, key=key)
        obj._class_info.value = cls._info

        for k, v in kwargs.iteritems():
            if k not in obj._rom_properties and k not in obj._rom_references:
                raise AttributeError("Unknown attribute %s" % k)
            setattr(obj, k, v)

        obj._on_create()

        return obj

    @classmethod
    def get(cls, connection=None, key=None):
        if connection is None or key is None:
            raise TypeError('get requires connection and key to be specified.')
        obj = cls(connection=connection, key=key)
        if not obj.exists():
            raise KeyError("Object not found: class=%s key=%s" % (cls, key))
        return obj

    def method_descriptor(self, method_name):
        method = getattr(self, method_name, None)
        if not hasattr(method, '__call__'):
            raise AttributeError("Unknown instance method %s for class %s"
                                 % (method_name, self.__class__.__name__))
        return {"object_key": self.key, "method_name": method_name}

    def subkey(self, *args):
        return _make_key(self.key, *args)

    def delete(self):
        for name in self._rom_properties.iterkeys():
            getattr(self, name).delete()
        for name, ref in self._rom_references.iteritems():
            if not ref.weak:
                try:
                    r = getattr(self, name)
                except RuntimeError:
                    pass
                else:
                    r.delete()
        self._class_info.delete()


def get_object(connection=None, key=None):
    if connection is None or key is None:
        raise TypeError("You must specify connection and key")

    class_info = connection.get(key)
    if class_info is None:
        raise KeyError("No object found in redis with key (%s)" % key)

    cls = Object.get_class(class_info)
    return cls(connection=connection, key=key)


def invoke_instance_method(connection, method_descriptor, **kwargs):
    obj = get_object(connection, method_descriptor['object_key'])
    method = getattr(obj, method_descriptor['method_name'], None)
    if method == None:
        raise AttributeError("Invalid method for class %s: %s"
                             % (obj.__class__.__name__,
                             method_descriptor["method_name"]))
    return method(**kwargs)
