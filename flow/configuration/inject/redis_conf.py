from flow.configuration.settings.injector import setting

import flow.interfaces
import injector
import redis


class RedisConfiguration(injector.Module):
    @injector.singleton
    @injector.provides(flow.interfaces.IStorage)
    @injector.inject(host=setting('redis.host', None),
            port=setting('redis.port', 6379),
            path=setting('redis.unix_socket_path', None))
    def provide_redis(self, host, port,  path):
        if path:
            return redis.Redis(unix_socket_path=path)
        else:
            return redis.Redis(host=host, port=port)
