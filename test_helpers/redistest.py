import os
import redis
import unittest

_redis_url_var = "FLOW_TEST_REDIS_URL"

class RedisTest(unittest.TestCase):
    def setUp(self):
        try:
            redis_host = os.environ[_redis_url_var]
        except KeyError:
            raise KeyError("You must set the %s environment variable to run "
                    "this test case" % _redis_url_var)

        self.conn = redis.Redis(redis_host)

        try:
            self.conn.ping()
        except redis.exceptions.ConnectionError as ex:
            raise RuntimeError("Failed to connect to redis, aborting test: %s" %
                    str(ex))

        self.conn.flushall()

    def tearDown(self):
        self.conn.flushall()
