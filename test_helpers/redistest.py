import os
import re
import redis
import subprocess
import tempfile
import time
import unittest


_TIMEOUT = 1
_MIN_REDIS_VERSION = (2, 6, 0)

class RedisTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.redis_unix_socket_path = tempfile.mktemp()

        cls.redis_server = start_redis(cls.redis_unix_socket_path)
        cls.conn = redis.Redis(unix_socket_path=cls.redis_unix_socket_path)

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
                time.time() - begin_time <= _TIMEOUT):
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
    validate_redis_version()
    return subprocess.Popen(construct_redis_command(unix_socket),
            stdout=open(os.devnull))


def config_path():
    return os.path.join(os.path.dirname(__file__), 'redis.conf')


def validate_redis_version():
    redis_version = get_redis_version()

    if cmp(redis_version, _MIN_REDIS_VERSION) < 0:
        raise RuntimeError('redis-server version error:  got %r, expected >= %r'
                % (redis_version, _MIN_REDIS_VERSION))


def get_redis_version():
    try:
        result = subprocess.check_output(['redis-server', '--version'])

    except OSError:
        raise RuntimeError("redis-server not in path")
    except subprocess.CalledProcessError:
        raise RuntimeError(
                'Failed to check redis-server version (might be too old)')

    return extract_version(result)


def extract_version(result):
    m = re.search(r'v\=(\d+)\.(\d+)\.(\d+)', result)

    if m is None:
        raise RuntimeError('Failed to parse redis version from: %s' % result)

    return tuple(map(int, m.groups()))


def construct_redis_command(unix_socket):
    cmd = ['redis-server', config_path(), "-- unixsocket", unix_socket]
    return cmd
