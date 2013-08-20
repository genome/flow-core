import csv
import flow.util.logannotator
import os
import re
import subprocess
import unittest


_VALID_LINE_REGEX = re.compile('^\[.+\] .*$')

def data_path(filename):
    return os.path.join(os.path.dirname(__file__),
            'logannotator_test_data', filename)


class LogAnnotatorTest(unittest.TestCase):
    def test_files(self):
        for filename, line_count in csv.reader(
                open(data_path('manifest.in'))):
            self.verify_file(filename, int(line_count))

    def verify_file(self, filename, line_count):
        output = subprocess.check_output(['python',
            flow.util.logannotator.__file__, 'cat',
            data_path(filename)], stderr=open('/dev/null', 'w'))

        self.check_output(filename, output, line_count)

    def check_output(self, filename, output, linecount):
        lines = output.split('\n')
        self.assertEqual(linecount + 2, len(lines),
                'Line count mismatch in %s -- expected %d, got %d:\n%s'
                % (filename, linecount + 2, len(lines), output))

        for line in lines:
            if line:
                self.assertTrue(_VALID_LINE_REGEX.match(line),
                        '%s: %s' % (filename, line))


if __name__ == '__main__':
    unittest.main()
