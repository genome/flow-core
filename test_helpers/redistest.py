from subprocess import Popen

import os
import redis
import tempfile
import time
import unittest


_redis_path = "FLOW_TEST_REDIS_PATH"
_timeout = 1


class RedisTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        unix_socket = tempfile.mktemp()

        cls.redis_server = start_redis(unix_socket)
        cls.conn = redis.Redis(unix_socket_path=unix_socket)

        cls._wait_for_connection()

    def tearDown(self):
        self.conn.flushall()

    @classmethod
    def tearDownClass(cls):
        cls.redis_server.terminate()

    @classmethod
    def _wait_for_connection(cls):
        begin_time = time.time()
        while not is_connected(cls.conn) and (
                time.time() - begin_time <= _timeout):
            time.sleep(.01)

        try:
            cls.conn.ping()
        except redis.exceptions.ConnectionError as ex:
            raise RuntimeError("Failed to connect to redis, aborting test: %s"
                    % ex)


def is_connected(conn):
    try:
        conn.ping()
    except redis.exceptions.ConnectionError:
        return False

    return True


def start_redis(unix_socket):
    return Popen(construct_redis_command(unix_socket), stdout=open(os.devnull))


def construct_redis_command(unix_socket):
    try:
        redis_path = os.environ[_redis_path]
    except KeyError:
        raise RuntimeError("You must set %s to run redis tests" % _redis_path)

    redis_executable = os.path.join(redis_path, 'redis-server')
    redis_conf = os.path.join(redis_path, 'redis.conf')
    cmd = [redis_executable, redis_conf, "-- unixsocket", unix_socket]
    return cmd
