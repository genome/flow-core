import flow.shell_command.lsf.executor

import json
import os
import re
import subprocess
import tempfile
import unittest


def lsf_available():
    return 'LSF_BINDIR' in os.environ


@unittest.skipIf(not lsf_available(), 'LSF not available')
class LSFExecutorSubmitTest(unittest.TestCase):
    def setUp(self):
        self.old_flow_config_path = os.environ.get('FLOW_CONFIG_PATH')
        os.environ['FLOW_CONFIG_PATH'] = os.path.dirname(__file__)

    def tearDown(self):
        if self.old_flow_config_path is not None:
            os.environ['FLOW_CONFIG_PATH'] = self.old_flow_config_path


    def executable(self):
        return [os.path.join(os.path.dirname(
            flow.shell_command.lsf.executor.__file__), 'executor.py')]

    def test_submit_success(self):
        data = {
            'command_line': [u'ls'],
            'net_key': 'abcdef',
            'color': 0,
            'color_group_idx': 0,

            'msg: dispatch_failure': 0,
            'msg: dispatch_success': 1,
            'msg: execute_begin':    2,
            'msg: execute_failure':  3,
            'msg: execute_success':  4,
        }

        stdin = tempfile.NamedTemporaryFile(delete=False)
        json.dump(data, stdin)
        stdin.close()

        output = subprocess.check_output(self.executable(),
                stdin=open(stdin.name))

        lines = output.split('\n')
        self.assertTrue(re.search('Job <\d+> is submitted to queue <\w+>.',
            lines[0]))
        self.assertTrue(int(lines[1]))

    def test_submit_failure_illegal_queue(self):
        data = {
            'command_line': [u'ls'],
            'net_key': 'abcdef',
            'color': 0,
            'color_group_idx': 0,

            'msg: dispatch_failure': 0,
            'msg: dispatch_success': 1,
            'msg: execute_begin':    2,
            'msg: execute_failure':  3,
            'msg: execute_success':  4,
            'lsf_options': {'queue': 'UNKNOWNQUEUE'},
        }

        stdin = tempfile.NamedTemporaryFile(delete=False)
        json.dump(data, stdin)
        stdin.close()

        self.assertEqual(1, subprocess.call(self.executable(),
            stdin=open(stdin.name), stderr=open('/dev/null', 'w')))



if '__main__' == __name__:
    unittest.main()
