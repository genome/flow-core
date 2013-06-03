import fakeredis
import unittest

class FakeRedisTest(unittest.TestCase):
    def setUp(self):
        self.conn = fakeredis.FakeRedis()

    def tearDown(self):
        self.conn.flushall()
