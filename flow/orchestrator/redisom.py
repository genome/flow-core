#!/usr/bin/env python

from uuid import uuid4
import redis
import json

KEY_DELIM = '/'

def json_enc(conn, obj):
    return json.dumps(obj)

def json_dec(conn, text):
    if text is None:
        return None
    else:
        return json.loads(text)


class RomIndexError(IndexError):
    pass

class NotInRedisError(KeyError):
    pass


class Property(object):
    def __init__(self, cls, **kwargs):
        if not issubclass(cls, Value):
            raise TypeError("Unknown redisom class %s" % str(cls))

        self.cls = cls
        self.kwargs = kwargs


class Value(object):
    def __init__(self, connection=None, key=None):
        if connection is None or key is None:
            raise TypeError("You must specify a connection and a key")
        self.connection = connection
        self.key = key

    def __str__(self):
        return str(self.value)

    @classmethod
    def create(cls, *args, **kwargs):
        return cls(*args, **kwargs)

    def delete(self):
        return self.connection.delete(self.key)

    def _value_getter(self):
        result = self.connection.get(self.key)
        if result is None:
            raise NotInRedisError("Redis has no data for key (%s)." %
                    (self.key))
        return result

    def _value_setter(self, new_value):
        return self.connection.set(self.key, new_value)

    value = property(_value_getter, _value_setter)

class Timestamp(Value):
    def _value_setter(self, new_value):
        raise AttributeError("You cannot set the .value of a Timestamp.")

    value = property(Value._value_getter, _value_setter)

    @property
    def now(self):
        sec, usec = self.connection.time()
        return "%d.%d" %(sec, usec)

    def set(self):
        now = self.now
        self.connection.set(self.key, now)
        return now

    def setnx(self):
        now = self.now
        if self.connection.setnx(self.key, now):
            return now
        return False


class Scalar(Value):
    def setnx(self, val):
        return self.connection.setnx(self.key, val)

    def incr(self, by=1):
        return self.connection.incr(self.key, by)

    def __str__(self):
        return self.value

    def __int__(self):
        return int(self.value)


class List(Value):
    def __getitem__(self, idx):
        try:
            idx = int(idx)
        except:
            raise TypeError("list indices must be integers, not str")

        pipe = self.connection.pipeline()
        pipe.llen(self.key)
        pipe.lindex(self.key, idx)
        size, value = pipe.execute()
        if idx >= size:
            raise RomIndexError ("list index out of range "
                    "(key=%s, size=%d, index=%d)" % (self.key, size, idx))
        return value

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
        # TODO: pipeline
        self.connection.delete(self.key)
        if val:
            return self.connection.sadd(self.key, *val)

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
            conn = self.connection
            encoder = self._value_encoder
            return dict((k, encoder(conn, v)) for k, v in d.iteritems())

    def _decode_dict(self, d):
        if self._value_encoder is None:
            return d
        else:
            conn = self.connection
            decoder = self._value_decoder
            return dict((k, decoder(conn, v)) for k, v in d.iteritems())

    def _encode_value(self, v):
        if self._value_encoder is None:
            return v
        else:
            return self._value_encoder(self.connection, v)

    def _decode_value(self, v):
        if self._value_decoder is None:
            return v
        else:
            return self._value_decoder(self.connection, v)

    def _decode_values(self, values):
        if self._value_decoder is None:
            return values
        else:
            conn = self.connection
            decoder = self._value_decoder
            return [decoder(conn, v) for v in values]

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
        pipe = self.connection.pipeline()
        pipe.hexists(self.key, hkey)
        pipe.hget(self.key, hkey)
        exists, value = pipe.execute()
        if not exists:
            raise KeyError("Hash (%s) has no key '%s'" % (self.key, hkey))
        return self._decode_value(value)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

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
    def __new__(meta, class_name, bases, class_dict):
        cls = type.__new__(meta, class_name, bases, class_dict)

        properties = {}
        cls._rom_properties = {}
        for base in bases:
            properties.update(base.__dict__)
            cls._rom_properties.update(getattr(base, "_rom_properties", {}))
        properties.update(class_dict)

        for name, value in properties.iteritems():
            if isinstance(value, Property):
                cls._rom_properties[name] = value
                delattr(cls, name)

        meta._class_registry[class_name] = cls
        return cls

    def get_registered_class(meta, class_name):
        return meta._class_registry[class_name]


class Object(object):
    __metaclass__ = ObjectMeta

    def __init__(self, connection=None, key=None):
        if connection is None:
            raise TypeError("You must specify a connection")
        if key == None:
            key = _make_key("/" + self.__module__, self.__class__.__name__,
                            uuid4().hex)

        self.__dict__.update({
            "key": key,
            "_rom_types": {},
            "_class_name": Scalar(connection=connection, key=key),
            "connection": connection,
        })


    def __getattr__(self, name):
        # fallback for when lookup in __dict__ fails
        try:
            return self._rom_types[name]
        except KeyError:
            try:
                propdef = self._rom_properties[name]
            except KeyError:
                raise AttributeError("No such property '%s' on class %s" %
                        (name, self.__class__.__name__))

            cls = propdef.cls
            prop = cls.create(connection=self.connection, key=self.subkey(name),
                               **propdef.kwargs)
            self._rom_types[name] = prop
            return prop

    def __setattr__(self, name, value):
        # is always called on attribute setting
        if name in self._rom_properties:
            getattr(self, name).value = value
        else:
            object.__setattr__(self, name, value)

    def __delattr__(self, name):
        getattr(self, name).delete()
        del self._rom_types[name]

    def exists(self):
        try:
            class_name = self._class_name.value
        except NotInRedisError:
            return False

        if class_name != self.__class__.__name__:
            raise TypeError("Class mismatch for object (%s)" % self.key)
        return True

    def _on_create(self):
        pass

    @classmethod
    def create(cls, connection=None, key=None, **kwargs):
        obj = cls(connection=connection, key=key)
        obj._class_name.value = cls.__name__

        for k, v in kwargs.iteritems():
            if k not in obj._rom_properties:
                raise AttributeError("Unknown attribute %s" % k)
            setattr(obj, k, v)

        obj._on_create()

        return obj

    @classmethod
    def get(cls, connection=None, key=None):
        if connection is None or key is None:
            raise RuntimeError('get requires connection and key to be specified.')
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
        self._class_name.delete()


def get_object(connection=None, key=None):
    if connection is None or key is None:
        raise TypeError("You must specify connection and key")

    class_name = connection.get(key)
    cls = Object.get_registered_class(class_name)
    return cls(connection=connection, key=key)


def invoke_instance_method(connection, method_descriptor, **kwargs):
    obj = get_object(connection, method_descriptor['object_key'])
    method = getattr(obj, method_descriptor['method_name'], None)
    if method == None:
        raise AttributeError("Invalid method for class %s: %s"
                             % (obj.__class__.__name__,
                             method_descriptor["method_name"]))
    return method(**kwargs)
