import redis

_REDIS_CONNECTION = None

def redis_storage_singleton(host=None, port=6379, db=0, path=None):
    global _REDIS_CONNECTION

    if _REDIS_CONNECTION is not None:
        return _REDIS_CONNECTION

    if path:
        _REDIS_CONNECTION = redis.Redis(unix_socket_path=path)
    else:
        _REDIS_CONNECTION = redis.Redis(host=host, port=port, db=db)

    return _REDIS_CONNECTION
