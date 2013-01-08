#!/usr/bin/env python

from uuid import uuid4

class RedisValueMeta(type): pass


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
    def __init__(self, connection, key):
        RedisValue.__init__(self, connection, key)

    def __getitem__(self, idx):
        return self.connection.lindex(self.key, idx)

    def __setitem__(self, idx, val):
        return self.connection.lset(self.key, idx, val)

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

    def __len__(self):
        return self.connection.llen(self.key)


class RedisSet(RedisValue):
    def __init__(self, connection, key):
        RedisValue.__init__(self, connection, key)

    @property
    def value(self):
        return self.connection.smembers(self.key)

    @value.setter
    def value(self, val):
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
        return self.connection.hmset(self.key, vals)

    def iteritems(self):
        return self.value.iteritems()

    def __setitem__(self, hkey, val):
        return self.connection.hset(self.key, hkey, val)

    def __getitem__(self, hkey):
        return self.connection.hget(self.key, hkey)

    def __len__(self):
        return self.connection.hlen(self.key)

    def keys(self):
        return self.connection.hkeys(self.key)

    def update(self, mapping):
        if len(mapping) == 0:
            return None
        return self.connection.hmset(self.key, mapping)


# ----8<-------
KEY_DELIM = '/'

def _make_key(*args):
    return KEY_DELIM.join(map(str, args))


def get_object(redis, key):
    class_key = _make_key(key, "_class_info")
    class_info = redis.hgetall(class_key)
    module = __import__(class_info['module'], fromlist=class_info['class'])
    return getattr(module, class_info['class'])(connection=redis, key=key)

def make_property_wrapper(name):
    private_name = "_" + name
    def getter(self):
        return getattr(self, private_name)

    def setter(self, value):
        getattr(self, private_name).value = value

    def deleter(self):
        getattr(self, private_name).delete()
        delattr(self, private_name)

    return property(getter, setter, deleter)


class StorableMeta(type):
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
                setattr(cls, name, make_property_wrapper(name))

        return cls


class RedisObject(object):
    __metaclass__ = StorableMeta

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
        
        for k, v in kwargs.iteritems():
            if k not in obj._redis_properties:
                raise AttributeError("Unknown attribute %s" %k)
            setattr(obj, k, v)

        return obj

    @classmethod
    def get(cls, connection, key):
        obj = cls(connection, key)
        if not obj.exists():
            raise KeyError("Object not found: class=%s key=%s" %(cls, key))
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

    def subkey(self, *args):
        return _make_key(self.key, *args)

    def child_object(self, *args):
        return get_object(self._connection, self.subkey(*args))
