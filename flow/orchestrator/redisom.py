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

def obj_enc(conn, obj):
    return obj.key

def obj_dec(conn, key):
    return get_object(conn, key)

class ValueMeta(type):
    pass


class Property(object):
    def __init__(self, cls, **kwargs):
        if not isinstance(cls, ValueMeta):
            raise TypeError("Unknown redisom class %s" % str(cls))

        self.cls = cls
        self.kwargs = kwargs


class Value(object):
    __metaclass__ = ValueMeta
    def __init__(self, connection, key):
        self.connection = connection
        self.key = key

    def delete(self):
        return self.connection.delete(self.key)

    def __repr__(self):
        return repr(self.value)


class Timestamp(Value):
    @property
    def value(self):
        return self.connection.get(self.key)

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

    def delete(self):
        return self.connection.delete(self.key)


class Scalar(Value):
    @property
    def value(self):
        return self.connection.get(self.key)

    @value.setter
    def value(self, val):
        return self.connection.set(self.key, val)

    def setnx(self, val):
        return self.connection.setnx(self.key, val)

    def increment(self, by=1):
        return self.connection.incr(self.key, by)

    def __str__(self):
        return self.value

    def __int__(self):
        return int(self.value)


class List(Value):
    def _make_index_error(self, size, idx):
        return IndexError("list range out of index (key=%s, size=%d, index=%d)"
                          % (self.key, size, idx))

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
            raise self._make_index_error(size, idx)
        return value

    def __setitem__(self, idx, val):
        try:
            return self.connection.lset(self.key, idx, val)
        except redis.ResponseError:
            raise self._make_index_error(len(self), idx)

    def __len__(self):
        return self.connection.llen(self.key)

    @property
    def value(self):
        return self.connection.lrange(self.key, 0, -1)

    @value.setter
    def value(self, val):
        self.connection.delete(self.key)
        if val:
            return self.connection.rpush(self.key, *val)

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
    @property
    def value(self):
        return self.connection.smembers(self.key)

    @value.setter
    def value(self, val):
        # TODO: pipeline
        self.connection.delete(self.key)
        if val:
            return self.connection.sadd(self.key, *val)

    def add(self, val):
        return self.connection.sadd(self.key, val)

    def update(self, vals):
        return self.connection.sadd(self.key, *vals)

    def remove(self, val):
        pipe = self.connection.pipeline()
        pipe.srem(self.key, val)
        pipe.scard(self.key)
        removed, size = pipe.execute()
        return removed, size

    def __iter__(self):
        return self.value.__iter__()

    def __len__(self):
        return self.connection.scard(self.key)


class Hash(Value):
    def __init__(self, connection, key, value_encoder=None, value_decoder=None):
        Value.__init__(self, connection, key)
        self._value_encoder = value_encoder
        self._value_decoder = value_decoder

    def _encode_dict(self, d):
        if not self._value_encoder:
            return d
        else:
            return dict((k, self._encode_value(v)) for k, v in d.iteritems())

    def _decode_dict(self, d):
        if not self._value_encoder:
            return d
        else:
            return dict((k, self._decode_value(v)) for k, v in d.iteritems())

    def _encode_value(self, v):
        if not self._value_encoder:
            return v
        else:
            return self._value_encoder(self.connection, v)

    def _decode_value(self, v):
        if not self._value_decoder:
            return v
        else:
            return self._value_decoder(self.connection, v)

    def increment(self, key, n):
        return self.connection.hincrby(self.key, key, n)

    @property
    def value(self):
        raw = self.connection.hgetall(self.key)
        return self._decode_dict(raw)

    @value.setter
    def value(self, vals):
        if vals:
            pipe = self.connection.pipeline()
            pipe.delete(self.key)
            pipe.hmset(self.key, self._encode_dict(vals))
            del_rv, set_rv = pipe.execute()
            return set_rv
        else:
            return self.connection.delete(self.key), 0

    def __setitem__(self, hkey, val):
        return self.connection.hset(self.key, hkey, self._encode_value(val))

    def __getitem__(self, hkey):
        pipe = self.connection.pipeline()
        pipe.hexists(self.key, hkey)
        pipe.hget(self.key, hkey)
        exists, value = pipe.execute()
        if not exists:
            raise KeyError(str(hkey))
        return self._decode_value(value)

    def get(self, key, default=None):
        pipe = self.connection.pipeline()
        pipe.hexists(self.key, hkey)
        pipe.hget(self.key, hkey)
        exists, value = pipe.execute()
        if not exists:
            return default
        return self._decode_value(value)

    def __delitem__(self, hkey):
        pipe = self.connection.pipeline()
        pipe.hexists(self.key, hkey)
        pipe.hdel(self.key, hkey)
        exists, rv = pipe.execute()
        if not exists:
            raise KeyError(str(hkey))
        return rv

    def __len__(self):
        return self.connection.hlen(self.key)

    def keys(self):
        return self.connection.hkeys(self.key)

    def values(self, keys=None):
        if keys == None:
            raw = self.connection.hvals(self.key)
        else:
            raw = self.connection.hmget(self.key, keys)

        return map(self._decode_value, raw)

    def update(self, mapping):
        if len(mapping) == 0:
            return None
        return self.connection.hmset(self.key, self._encode_dict(mapping))

    def iteritems(self):
        return self.value.iteritems()


def _make_key(*args):
    return KEY_DELIM.join(map(str, args))


def _make_private_name(name):
    return "_rom_" + name


class ObjectMeta(type):
    def __new__(meta, class_name, bases, class_dict):
        cls = type.__new__(meta, class_name, bases, class_dict)

        properties = {}
        cls._redis_properties = {}
        for base in bases:
            properties.update(base.__dict__)
            cls._redis_properties.update(getattr(base, "_redis_properties", {}))
        properties.update(class_dict)

        for name, value in properties.iteritems():
            if isinstance(value, Property):
                cls._redis_properties[name] = value
                delattr(cls, name)

        return cls


class Object(object):
    __metaclass__ = ObjectMeta

    def __init__(self, connection, key):
        if key == None:
            key = _make_key("/" + self.__module__, self.__class__.__name__,
                            uuid4().hex)

        self.__dict__.update({
            "key": key,
            "_redis_property_instances": {},
            "_class_info": Hash(connection, key),
            "_connection": connection,
        })

    def __getattr__(self, name):
        try:
            return self._redis_property_instances[name]
        except KeyError:
            try:
                propdef = self._redis_properties[name]
            except KeyError:
                raise AttributeError("No such property %s on class %s" %
                        (name, self.__class__.__name__))
            prop = propdef.cls(connection=self._connection, key=self.subkey(name),
                               **propdef.kwargs)
            self._redis_property_instances[name] = prop
            return prop

    def __setattr__(self, name, value):
        getattr(self, name).value = value

    def __delattr__(self, name):
        getattr(self, name).delete()
        del self._redis_property_instances[name]

    def exists(self):
        cinfo = self._class_info.value
        if "module" in cinfo and "class" in cinfo:
            if (self.__class__.__module__ != self._class_info["module"]
                or self.__class__.__name__ != self._class_info["class"]):
                raise TypeError("Class mismatch for object %s" % self.key)
        else:
            return False
        return True

    def _on_create(self):
        pass

    @classmethod
    def create(cls, connection=None, key=None, **kwargs):
        obj = cls(connection, key)
        obj._class_info.update({"module": cls.__module__,
                                "class": cls.__name__})

        for k, v in kwargs.iteritems():
            if k not in obj._redis_properties:
                raise AttributeError("Unknown attribute %s" % k)
            setattr(obj, k, v)

        obj._on_create()

        return obj

    @classmethod
    def get(cls, connection, key):
        obj = cls(connection, key)
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


def get_object(redis, key):
    class_info = redis.hgetall(key)
    if 'module' not in class_info or 'class' not in class_info:
        raise KeyError("Requested object (%s) not found" % key)
    module = __import__(class_info['module'], fromlist=class_info['class'])
    return getattr(module, class_info['class'])(connection=redis, key=key)


def invoke_instance_method(connection, method_descriptor, **kwargs):
    obj = get_object(connection, method_descriptor['object_key'])
    method = getattr(obj, method_descriptor['method_name'], None)
    if method == None:
        raise AttributeError("Invalid method for class %s: %s"
                             % (obj.__class__.__name__,
                             method_descriptor["method_name"]))
    return method(**kwargs)
