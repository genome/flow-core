import os
import redis
import unittest
import uuid
from subprocess import Popen
import time

_redis_path = "FLOW_TEST_REDIS_PATH"
_timeout = 1

class RedisTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.unix_socket = os.path.join("/tmp", str(uuid.uuid4()))
        cmd = construct_redis_command(cls.unix_socket)
        cls.redis_server = Popen(cmd, stdout=open(os.devnull))

    def setUp(self):
        self.conn = redis.Redis(unix_socket_path=self.unix_socket)
        wait_for_connection(self.conn)
        self.conn.flushall()

    def tearDown(self):
        self.conn.flushall()

    @classmethod
    def tearDownClass(cls):
        cls.redis_server.terminate()
        pass

def wait_for_connection(conn):
    begin_time = time.time()
    while not is_connected(conn) and (time.time() - begin_time <= _timeout):
        time.sleep(.01)

    try:
        conn.ping()
    except redis.exceptions.ConnectionError as ex:
        raise RuntimeError("Failed to connect to redis, aborting test: %s" %
                str(ex))
        

def is_connected(conn):
    try:
        conn.ping()
    except redis.exceptions.ConnectionError:
        return False

    return True
    

def construct_redis_command(unix_socket):
    try:
        redis_path = os.environ[_redis_path]
    except KeyError:
        raise RuntimeError("You must set %s to run redis tests" % _redis_path)

    redis_executable = os.path.join(redis_path, 'redis-server')
    redis_conf = os.path.join(redis_path, 'redis.conf')
    cmd = [redis_executable, redis_conf, "-- unixsocket", unix_socket]
    return cmd
