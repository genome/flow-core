import mock
import unittest

import os
from uuid import uuid4
from flow.util import stats

class StatsTest(unittest.TestCase):
    def test_assemble_label(self):
        self.assertEqual(stats.assemble_label(['a', 'b'], 'c'), 'a.b.c')
