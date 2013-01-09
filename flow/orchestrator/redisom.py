#!/usr/bin/env python

from uuid import uuid4
import redis

KEY_DELIM = '/'


class RedisValueMeta(type):
    pass


class RedisValue(object):
    __metaclass__ = RedisValueMeta
    def __init__(self, connection, key):
        self.connection = connection
        self.key = key

    def clear(self):
        return self.connection.delete(self.key)

    def delete(self):
        self.connection.delete(self.key)

    def __repr__(self):
        return repr(self.value)


class RedisScalar(RedisValue):
    def __init__(self, connection, key):
        RedisValue.__init__(self, connection, key)

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


class RedisList(RedisValue):
    def _make_index_error(self, size, idx):
        return IndexError("list range out of index (key=%s, size=%d, index=%d)"
                          % (self.key, size, idx))

    def __init__(self, connection, key):
        RedisValue.__init__(self, connection, key)

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


class RedisSet(RedisValue):
    def __init__(self, connection, key):
        RedisValue.__init__(self, connection, key)

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
        return self.connection.srem(self.key, val)

    def __iter__(self):
        return self.value.__iter__()

    def __len__(self):
        return self.connection.scard(self.key)


class RedisHash(RedisValue):
    def __init__(self, connection, key):
        RedisValue.__init__(self, connection, key)

    @property
    def value(self):
        return self.connection.hgetall(self.key)

    @value.setter
    def value(self, vals):
        pipe = self.connection.pipeline()
        pipe.delete(self.key)
        pipe.hmset(self.key, vals)
        del_rv, set_rv = pipe.execute()
        return set_rv

    def __setitem__(self, hkey, val):
        return self.connection.hset(self.key, hkey, val)

    def __getitem__(self, hkey):
        pipe = self.connection.pipeline()
        pipe.hexists(self.key, hkey)
        pipe.hget(self.key, hkey)
        exists, value = pipe.execute()
        if not exists:
            raise KeyError(str(hkey))
        return value

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

    def values(self):
        return self.connection.hvals(self.key)

    def update(self, mapping):
        if len(mapping) == 0:
            return None
        return self.connection.hmset(self.key, mapping)

    def iteritems(self):
        return self.value.iteritems()


def _make_key(*args):
    return KEY_DELIM.join(map(str, args))


def _make_property_wrapper(name):
    private_name = "_" + name
    def getter(self):
        return getattr(self, private_name)

    def setter(self, value):
        getattr(self, private_name).value = value

    def deleter(self):
        getattr(self, private_name).delete()
        delattr(self, private_name)

    return property(getter, setter, deleter)


class RedisObjectMeta(type):
    def __new__(meta, class_name, bases, class_dict):
        cls = type.__new__(meta, class_name, bases, class_dict)

        properties = {}
        cls._redis_properties = {}
        for base in bases:
            properties.update(base.__dict__)
            cls._redis_properties.update(getattr(base, "_redis_properties", {}))
        properties.update(class_dict)

        for name, value in properties.iteritems():
            if isinstance(value, RedisValueMeta):
                cls._redis_properties[name] = value
                setattr(cls, name, _make_property_wrapper(name))

        return cls


class RedisObject(object):
    __metaclass__ = RedisObjectMeta

    def exists(self):
        cinfo = self._class_info.value
        if "module" in cinfo and "class" in cinfo:
            if (self.__class__.__module__ != self._class_info["module"]
                or self.__class__.__name__ != self._class_info["class"]):
                raise RuntimeError("Class mismatch for object %s" %self.key)
        else:
            return False

    @classmethod
    def create(cls, connection=None, key=None, **kwargs):
        obj = cls(connection, key)
        obj._class_info.update({"module": cls.__module__,
                                "class": cls.__name__})

        for k, v in kwargs.iteritems():
            if k not in obj._redis_properties:
                raise AttributeError("Unknown attribute %s" % k)
            setattr(obj, k, v)

        return obj

    @classmethod
    def get(cls, connection, key):
        obj = cls(connection, key)
        if not obj.exists():
            raise KeyError("Object not found: class=%s key=%s" % (cls, key))
        return obj

    def __init__(self, connection, key):
        self._connection = connection
        if key == None:
            key = _make_key("/" + self.__class__.__name__, uuid4().hex)
        self.key = key
        self._class_info = RedisHash(connection, self.key)

        for name, value in self._redis_properties.iteritems():
            private_name = "_" + name
            pobj = value(connection=connection, key=self.subkey(name))
            setattr(self, private_name, pobj)

    def method_descriptor(self, method_name):
        method = getattr(self, method_name, None)
        if not hasattr(method, '__call__'):
            raise AttributeError("Unknown instance method %s for class %s"
                                 % (method_name, self.__class__.__name__))
        return {"object_key": self.key, "method_name": method_name}

    def subkey(self, *args):
        return _make_key(self.key, *args)

    def child_object(self, *args):
        return get_object(self._connection, self.subkey(*args))


def get_object(redis, key):
    class_info = redis.hgetall(key)
    module = __import__(class_info['module'], fromlist=class_info['class'])
    return getattr(module, class_info['class'])(connection=redis, key=key)


def invoke_instance_method(connection, method_descriptor, **kwargs):
    obj = get_object(connection, method_descriptor["object_key"])
    method = getattr(obj, method_descriptor["method_name"], None)
    if method == None:
        raise AttributeError("Invalid method for class %s: %s"
                             % (obj.__class__.__name__,
                             method_descriptor["method_name"]))
    return method(**kwargs)
