from flow.configuration.settings.injector import setting
from injector import inject, provides, singleton
from flow.interfaces import IStorage
import redis

@singleton
@provides(IStorage)
@inject(host=setting('redis.host'), port=setting('redis.port'),
        path=setting('redis.unix_socket_path'))
def redis_storage_singleton(self, host=None, port=6379, db=0, path=None):
    if path:
        return redis.Redis(unix_socket_path=path)
    else:
        return redis.Redis(host=host, port=port, db=db)
