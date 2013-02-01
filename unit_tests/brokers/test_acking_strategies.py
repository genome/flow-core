import unittest
try:
    from unittest import mock
except ImportError:
    import mock

import itertools
from flow.brokers import acking_strategies

class TestImmediate(unittest.TestCase):
    def setUp(self):
        self.immediate = acking_strategies.Immediate()
        self.immediate.reset()

    def test_initialization(self):
        self.assertEqual(0, self.immediate._largest_receive_tag)

    def test_add_receive_tag(self):
        tag = mock.Mock()
        self.immediate.add_receive_tag(tag)
        self.assertEqual(tag, self.immediate._largest_receive_tag)

    def test_reset(self):
        tag = mock.Mock()
        self.immediate.add_receive_tag(tag)
        self.immediate.reset()
        self.assertEqual(0, self.immediate._largest_receive_tag)

    def test_pop_ackable_receive_tags(self):
        tag = mock.Mock()
        self.immediate.add_receive_tag(tag)

        tags, multiple = self.immediate.pop_ackable_receive_tags()
        self.assertEqual([tag], tags)

        self.assertEqual(True, multiple)

class TestTagRelationships(unittest.TestCase):
    TEST_DATA = [
            {'label': 'new receive tag is initially ackable',
             'receive_tags': [7],
             'publish_tags': [[]],
             'post_setup_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 0
             },
             'confirms_received': [],
             'post_confirm_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 0
             },
             'pop_ackable_tags': [7],
             'pop_multiple': False,
            },

            {'label': 'add publish tag makes receive tag non ackable',
             'receive_tags': [7],
             'publish_tags': [[3]],
             'post_setup_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 1,
                 'unconfirmed_publish_tags': 1
             },
             'confirms_received': [],
             'post_confirm_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 1,
                 'unconfirmed_publish_tags': 1
             },
             'pop_ackable_tags': [],
             'pop_multiple': False,
            },

            {'label': 'confirming publish tag makes receive tag ackable',
             'receive_tags': [7],
             'publish_tags': [[3]],
             'post_setup_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 1,
                 'unconfirmed_publish_tags': 1
             },
             'confirms_received': [(3, False)],
             'post_confirm_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 0
             },
             'pop_ackable_tags': [7],
             'pop_multiple': False,
            },

            {'label': '2 receives gives multiple ack',
             'receive_tags': [7, 8],
             'publish_tags': [[], [], []],
             'post_setup_stats': {
                 'ackable_receive_tags': 2,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 0
             },
             'confirms_received': [],
             'post_confirm_stats': {
                 'ackable_receive_tags': 2,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 0
             },
             'pop_ackable_tags': [8],
             'pop_multiple': True,
            },

            {'label': 'later receive tags begin ackable',
             'receive_tags': [7, 8],
             'publish_tags': [[3], []],
             'post_setup_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 1,
                 'unconfirmed_publish_tags': 1
             },
             'confirms_received': [],
             'post_confirm_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 1,
                 'unconfirmed_publish_tags': 1
             },
             'pop_ackable_tags': [8],
             'pop_multiple': False,
            },

            # XXX Still need more test cases
    ]

    def add_tags(self, tr, receive_tags=None, publish_tags=None):
        for rt, pts in itertools.izip(receive_tags, publish_tags):
            tr.add_receive_tag(rt)
            for pt in pts:
                tr.add_publish_tag(receive_tag=rt, publish_tag=pt)

    def confirm_tags(self, tr, publish_tags):
        for pt, multiple in publish_tags:
            tr.remove_publish_tag(pt, multiple=multiple)

    def test_all_the_things(self):
        error_message_templates = {
            'setup_stats':
                "stats mismatch after setup for '%s'\nexpected: %s\ngot: %s",
            'confirm_stats':
                "stats mismatch after confirm for '%s'\nexpected: %s\ngot: %s",
            'ackable_tags':
                "ackable receive tags mismatch for '%s'\nexpected: %s\ngot: %s",
            'multiple':
                "multiple flag mismatch for '%s'\nexpected: %s\ngot: %s",
        }

        for data in self.TEST_DATA:
            tr = acking_strategies.TagRelationships()

            label = data['label']
            self.add_tags(tr, receive_tags=data['receive_tags'],
                    publish_tags=data['publish_tags'])
            self.assertEqual(data['post_setup_stats'], tr.stats,
                    msg=error_message_templates['setup_stats'] % (
                        label, data['post_setup_stats'], tr.stats))

            self.confirm_tags(tr, data['confirms_received'])
            self.assertEqual(data['post_confirm_stats'], tr.stats,
                    msg=error_message_templates['confirm_stats'] % (
                        label, data['post_confirm_stats'], tr.stats))

            ackable_tags, multiple = tr.pop_ackable_receive_tags()

            self.assertEqual(data['pop_ackable_tags'], ackable_tags,
                    msg=error_message_templates['ackable_tags'] % (
                        label, data['pop_ackable_tags'], ackable_tags))
            self.assertEqual(data['pop_multiple'], multiple,
                    msg=error_message_templates['multiple'] % (
                        label, data['pop_multiple'], multiple))
