import os
import subprocess
import unittest


EXECUTABLE = os.path.join(os.path.dirname(__file__), 'handler_base_runner.py')
PRETEND_CAT = os.path.join(os.path.dirname(__file__), 'pretend_cat.py')

class HandlerBaseTest(unittest.TestCase):
    def test_success(self):
        subprocess.check_call([EXECUTABLE, '--expect-success', PRETEND_CAT],
                stdout=open('/dev/null', 'w'))

#    def test_failure(self):
#        subprocess.check_call([EXECUTABLE, 'false'],
#                stderr=open('/dev/null', 'w'))


if __name__ == '__main__':
    unittest.main()
