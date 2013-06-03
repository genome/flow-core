import fakeredis
import time
import unittest


class FakeRedisAdapter(fakeredis.FakeRedis):
    def time(self):
        now = time.time()
        sec = int(now)
        usec = int((now - sec)*1e6)
        return sec, usec


class FakeRedisTest(unittest.TestCase):
    def setUp(self):
        self.conn = FakeRedisAdapter()

    def tearDown(self):
        self.conn.flushall()
