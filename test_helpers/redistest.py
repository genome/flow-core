import os
import redis
import unittest

_redis_url_var = "FLOW_TEST_REDIS_URL"
_redis_unix_socket_var = "FLOW_TEST_REDIS_SOCKET"

class RedisTest(unittest.TestCase):
    def setUp(self):
        try:
            redis_ud_socket = os.environ[_redis_unix_socket_var]
            self.conn = redis.Redis(unix_socket_path=redis_ud_socket)
        except KeyError:
            try:
                redis_host = os.environ[_redis_url_var]
                self.conn = redis.Redis(redis_host)
            except KeyError:
                raise KeyError("You must set either %s or %s to run "
                        "this test case" % (_redis_url_var, _redis_unix_socket_var))


        try:
            self.conn.ping()
        except redis.exceptions.ConnectionError as ex:
            raise RuntimeError("Failed to connect to redis, aborting test: %s" %
                    str(ex))

        self.conn.flushall()

    def tearDown(self):
        self.conn.flushall()
