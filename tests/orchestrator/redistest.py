#!/usr/bin/env python

from fakeredis import FakeRedis
import time

class RedisTest(FakeRedis):
    # fakeredis has no time()
    def time(self):
        now = time.time()
        sec = int(now)
        usec = int((now - sec)*1e6)
        return sec, usec
