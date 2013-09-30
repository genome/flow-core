from flow.configuration.settings.injector import setting

import flow.interfaces
import injector
import os
import redis
import subprocess
import sys
import tempfile
import time


class RedisConfiguration(injector.Module):
    @injector.singleton
    @injector.provides(flow.interfaces.IStorage)
    @injector.inject(host=setting('redis.host', None),
            port=setting('redis.port', 6379),
            path=setting('redis.unix_socket_path', None))
    def provide_redis(self, host, port,  path):
        if 'FLOW_REDIS_SOCKET' in os.environ:
            path = os.environ['FLOW_REDIS_SOCKET']

        if path:
            return redis.Redis(unix_socket_path=path)
        else:
            return redis.Redis(host=host, port=port)

class LocalRedisConfiguration(injector.Module):
    @injector.singleton
    @injector.provides(flow.interfaces.IStorage)
    def provide_redis(self):
        _, unix_socket = tempfile.mkstemp()
        subprocess.Popen(['flow-redis-server', '--unixsocket', unix_socket],
                stdout=sys.stderr)

        os.environ['FLOW_REDIS_SOCKET'] = unix_socket

        conn = redis.Redis(unix_socket_path=unix_socket)
        _wait_for_connection(conn)

        return conn

_TIMEOUT=30
def _wait_for_connection(conn):
    begin_time = time.time()
    while not is_connected(conn) and (
            time.time() - begin_time <= _TIMEOUT):
        time.sleep(.25)

    try:
        conn.ping()
    except redis.exceptions.ConnectionError as ex:
        raise RuntimeError("Failed to connect to redis, aborting test: %s"
                % ex)

def is_connected(conn):
    try:
        print 'pinging...'
        conn.ping()
    except redis.exceptions.ConnectionError:
        return False

    return True
