from flow.brokers import acking_strategies

import itertools
import mock
import unittest


class TestTagRelationships(unittest.TestCase):
    ACK_TEST_DATA = [
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
             'publish_tags': [[], []],
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

            {'label': '2 publish tags 1 single confirm',
             'receive_tags': [7],
             'publish_tags': [[3, 4]],
             'post_setup_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 1,
                 'unconfirmed_publish_tags': 2
             },
             'confirms_received': [(4, False)],
             'post_confirm_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 1,
                 'unconfirmed_publish_tags': 1
             },
             'pop_ackable_tags': [],
             'pop_multiple': False,
            },

            {'label': '2 publish tags 1 multiple confirm',
             'receive_tags': [7],
             'publish_tags': [[3, 4]],
             'post_setup_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 1,
                 'unconfirmed_publish_tags': 2
             },
             'confirms_received': [(4, True)],
             'post_confirm_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 0
             },
             'pop_ackable_tags': [7],
             'pop_multiple': False,
            },

            {'label': '2 publish tags 2 single confirms',
             'receive_tags': [7],
             'publish_tags': [[3, 4]],
             'post_setup_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 1,
                 'unconfirmed_publish_tags': 2
             },
             'confirms_received': [(4, False), (3, False)],
             'post_confirm_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 0
             },
             'pop_ackable_tags': [7],
             'pop_multiple': False,
            },

            {'label': '3 receive tags multiple publish tags single confirm',
             'receive_tags': [6, 7, 8],
             'publish_tags': [[], [3, 4], [5, 6]],
             'post_setup_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 2,
                 'unconfirmed_publish_tags': 4
             },
             'confirms_received': [(4, False)],
             'post_confirm_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 2,
                 'unconfirmed_publish_tags': 3
             },
             'pop_ackable_tags': [6],
             'pop_multiple': False,
            },

            {'label': '3 receive tags multiple publish tags 1 multiple confirm',
             'receive_tags': [6, 7, 8],
             'publish_tags': [[], [3, 4], [5, 6]],
             'post_setup_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 2,
                 'unconfirmed_publish_tags': 4
             },
             'confirms_received': [(5, True)],
             'post_confirm_stats': {
                 'ackable_receive_tags': 2,
                 'non_ackable_receive_tags': 1,
                 'unconfirmed_publish_tags': 1
             },
             'pop_ackable_tags': [7],
             'pop_multiple': True,
            },

            {'label': '3 receive tags multiple publish tags 1 big multiple confirm',
             'receive_tags': [6, 7, 8],
             'publish_tags': [[], [3, 4], [5, 6]],
             'post_setup_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 2,
                 'unconfirmed_publish_tags': 4
             },
             'confirms_received': [(6, True)],
             'post_confirm_stats': {
                 'ackable_receive_tags': 3,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 0
             },
             'pop_ackable_tags': [8],
             'pop_multiple': True,
            },

            {'label': 'smallest unackable tag is in early middle of list',
             'receive_tags': [6, 7, 8, 9],
             'publish_tags': [[2], [4], [], [6]],
             'post_setup_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 3,
                 'unconfirmed_publish_tags': 3
             },
             'confirms_received': [(2, False)],
             'post_confirm_stats': {
                 'ackable_receive_tags': 2,
                 'non_ackable_receive_tags': 2,
                 'unconfirmed_publish_tags': 2
             },
             'pop_ackable_tags': [6, 8],
             'pop_multiple': False,
            },

            {'label': 'smallest unackable tag is in late middle of list',
             'receive_tags': [6, 7, 8, 9, 10, 11, 12],
             'publish_tags': [[6], [], [], [9], [10], [], [12]],
             'post_setup_stats': {
                 'ackable_receive_tags': 3,
                 'non_ackable_receive_tags': 4,
                 'unconfirmed_publish_tags': 4
             },
             'confirms_received': [(9, True)],
             'post_confirm_stats': {
                 'ackable_receive_tags': 5,
                 'non_ackable_receive_tags': 2,
                 'unconfirmed_publish_tags': 2
             },
             'pop_ackable_tags': [9, 11],
             'pop_multiple': True,
            },

    ]

    REJECT_TEST_DATA = [
            {'label': 'immediate reject is never acked',
             'receive_tags': [7],
             'publish_tags': [[]],
             'post_setup_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 0
             },
             'reject_tags': [7],
             'post_reject_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 0
             },
             'confirms_received': [],
             'post_confirm_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 0
             },
             'pop_ackable_tags': [],
             'pop_multiple': False,
            },

            {'label': 'reject with publish tags can still receive confirms',
             'receive_tags': [7],
             'publish_tags': [[13]],
             'post_setup_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 1,
                 'unconfirmed_publish_tags': 1
             },
             'reject_tags': [7],
             'post_reject_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 1
             },
             'confirms_received': [(13, False)],
             'post_confirm_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 0
             },
             'pop_ackable_tags': [],
             'pop_multiple': False,
            },

            {'label': "reject with publish tags doesn't break ackable tags",
             'receive_tags': [7, 8],
             'publish_tags': [[13], []],
             'post_setup_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 1,
                 'unconfirmed_publish_tags': 1
             },
             'reject_tags': [7],
             'post_reject_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 1
             },
             'confirms_received': [(13, False)],
             'post_confirm_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 0
             },
             'pop_ackable_tags': [8],
             'pop_multiple': False,
            },

            {'label': "reject with publish tags doesn't break confirmable tags",
             'receive_tags': [7, 8],
             'publish_tags': [[13], [22]],
             'post_setup_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 2,
                 'unconfirmed_publish_tags': 2
             },
             'reject_tags': [7],
             'post_reject_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 1,
                 'unconfirmed_publish_tags': 2
             },
             'confirms_received': [(13, False)],
             'post_confirm_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 1,
                 'unconfirmed_publish_tags': 1
             },
             'pop_ackable_tags': [],
             'pop_multiple': False,
            },

            {'label': "reject with publish tags doesn't break confirmed tags",
             'receive_tags': [7, 8],
             'publish_tags': [[13], [22]],
             'post_setup_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 2,
                 'unconfirmed_publish_tags': 2
             },
             'reject_tags': [7],
             'post_reject_stats': {
                 'ackable_receive_tags': 0,
                 'non_ackable_receive_tags': 1,
                 'unconfirmed_publish_tags': 2
             },
             'confirms_received': [(22, False)],
             'post_confirm_stats': {
                 'ackable_receive_tags': 1,
                 'non_ackable_receive_tags': 0,
                 'unconfirmed_publish_tags': 1
             },
             'pop_ackable_tags': [8],
             'pop_multiple': False,
            },

    ]

    def add_tags(self, tr, receive_tags=None, publish_tags=None):
        for rt, pts in itertools.izip(receive_tags, publish_tags):
            tr.add_receive_tag(rt)
            for pt in pts:
                tr.add_publish_tag(receive_tag=rt, publish_tag=pt)

    def reject_tags(self, tr, receive_tags):
        for tag in receive_tags:
            tr.remove_receive_tag(tag)

    def confirm_tags(self, tr, publish_tags):
        for pt, multiple in publish_tags:
            tr.remove_publish_tag(pt, multiple=multiple)

    def test_acking(self):
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

        for data in self.ACK_TEST_DATA:
            print "Testing %s" % data['label']
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

    def test_rejecting(self):
        error_message_templates = {
            'setup_stats':
                "stats mismatch after setup for '%s'\nexpected: %s\ngot: %s",
            'reject_stats':
                "stats mismatch after reject for '%s'\nexpected: %s\ngot: %s",
            'confirm_stats':
                "stats mismatch after confirm for '%s'\nexpected: %s\ngot: %s",
            'ackable_tags':
                "ackable receive tags mismatch for '%s'\nexpected: %s\ngot: %s",
            'multiple':
                "multiple flag mismatch for '%s'\nexpected: %s\ngot: %s",
        }

        for data in self.REJECT_TEST_DATA:
            print "Testing %s" % data['label']
            tr = acking_strategies.TagRelationships()

            label = data['label']
            self.add_tags(tr, receive_tags=data['receive_tags'],
                    publish_tags=data['publish_tags'])
            self.assertEqual(data['post_setup_stats'], tr.stats,
                    msg=error_message_templates['setup_stats'] % (
                        label, data['post_setup_stats'], tr.stats))

            self.reject_tags(tr, data['reject_tags'])
            self.assertEqual(data['post_reject_stats'], tr.stats,
                    msg=error_message_templates['reject_stats'] % (
                        label, data['post_reject_stats'], tr.stats))

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
