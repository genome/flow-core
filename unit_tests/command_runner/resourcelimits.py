import flow.command_runner.resourcelimits as rl
import unittest

class TestResourceLimits(unittest.TestCase):
    def test_default_args(self):
        res = rl.ResourceLimits(cpu_time=100, core_file_size=10485760)
        self.assertEqual(100, res.cpu_time)
        self.assertEqual(10485760, res.core_file_size)
        others = set(res._fields) - set(['cpu_time', 'core_file_size'])
        for pname in others:
            self.assertIsNone(getattr(res, pname))

if __name__ == "__main__":
    unittest.main()
