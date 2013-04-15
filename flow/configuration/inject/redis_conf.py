import flow.interfaces
import injector
import redis

class RedisConfiguration(injector.Module):
    @injector.singleton
    @injector.provides(flow.interfaces.IStorage)
    @injector.inject(host=injector.Setting('redis.host'),
            port=injector.Setting('redis.port'),
            path=injector.Setting('redis.unix_socket_path'))
    def provide_redis(self, host=None, port=6379,  path=None):
        if path:
            return redis.Redis(unix_socket_path=path)
        else:
            return redis.Redis(host=host, port=port)
