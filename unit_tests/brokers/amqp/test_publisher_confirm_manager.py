import unittest
try:
    from unittest import mock
except:
    import mock

from twisted.internet import defer
from pika.spec import Basic

from flow.brokers.amqp.publisher_confirm_manager import PublisherConfirmManager
from flow import exit_codes

class TestPublisherConfirmManager(unittest.TestCase):
    def setUp(self):
        channel = mock.Mock()
        channel.channel_number = mock.Mock()
        channel.confirm_delivery = mock.Mock()
        channel.callbacks = mock.Mock()
        channel.callbacks.add = mock.Mock()
        self.channel = channel

        self.pcm = PublisherConfirmManager(channel)

    def test_init(self):
        c = self.channel
        self.assertEqual(c.confirm_delivery.call_count, 1)
        c.callbacks.add.assert_any_call(c.channel_number, Basic.Ack,
                self.pcm._on_publisher_confirm_ack, one_shot=False)
        c.callbacks.add.assert_any_call(c.channel_number, Basic.Nack,
                self.pcm._on_publisher_confirm_nack, one_shot=False)
        self.assertEqual(c.callbacks.add.call_count, 2)

        self.assertEquals(len(self.pcm._confirm_tags), 0)
        self.assertEquals(len(self.pcm._confirm_deferreds), 0)


    def test_private_on_publisher_confirm_ack(self):
        method_frame = mock.Mock()
        method_frame.method = mock.Mock()
        publish_tag = 1234
        multiple = object()
        method_frame.method.delivery_tag = publish_tag
        method_frame.method.multiple = multiple

        self.pcm._fire_confirm_deferreds = mock.Mock()

        self.pcm._on_publisher_confirm_ack(method_frame)
        self.pcm._fire_confirm_deferreds.assert_called_once_with(publish_tag=publish_tag,
                multiple=multiple)

    def test_private_on_publisher_confirm_nack(self):
        method_frame = mock.Mock()
        method_frame.method = mock.Mock()
        publish_tag = 1234
        multiple = object()
        method_frame.method.delivery_tag = publish_tag
        method_frame.method.multiple = multiple

        fake_exit = mock.Mock()
        with mock.patch('os._exit', new=fake_exit):
            self.pcm._on_publisher_confirm_nack(method_frame)
            fake_exit.assert_called_once_with(exit_codes.EXECUTE_SYSTEM_FAILURE)

    def test_add_confirm_deferred(self):
        test_confirm_deferreds = {
                mock.Mock(): mock.Mock(),
                mock.Mock(): mock.Mock(),
                mock.Mock(): mock.Mock()
        }
        for i, (publish_tag, deferred) in enumerate(test_confirm_deferreds.items()):
            self.pcm.add_confirm_deferred(publish_tag, deferred)
            self.assertEqual(len(self.pcm._confirm_tags), i+1)
            self.assertEqual(len(self.pcm._confirm_deferreds), i+1)
            self.assertTrue(self.pcm._confirm_deferreds[publish_tag] is deferred)

        # repeated adds do nothing
        for i, (publish_tag, deferred) in enumerate(test_confirm_deferreds.items()):
            self.pcm.add_confirm_deferred(publish_tag, deferred)
            self.assertEqual(len(self.pcm._confirm_tags), len(test_confirm_deferreds))
            self.assertEqual(len(self.pcm._confirm_deferreds), len(test_confirm_deferreds))
            self.assertTrue(self.pcm._confirm_deferreds[publish_tag] is deferred)

    def test_remove_confirm_deferred(self):
        # removing non-existing gets key error
        with self.assertRaises(ValueError):
            self.pcm.remove_confirm_deferred(0)

        # normal removal
        test_confirm_deferreds = {
                mock.Mock(): mock.Mock(),
                mock.Mock(): mock.Mock(),
                mock.Mock(): mock.Mock()
        }
        for publish_tag, deferred in test_confirm_deferreds.items():
            self.pcm.add_confirm_deferred(publish_tag, deferred)

        for publish_tag, deferred in test_confirm_deferreds.items():
            self.assertTrue(publish_tag in self.pcm._confirm_tags)
            self.assertIs(self.pcm._confirm_deferreds[publish_tag], deferred)

            self.pcm.remove_confirm_deferred(publish_tag)

            self.assertFalse(publish_tag in self.pcm._confirm_deferreds)
            self.assertFalse(publish_tag in self.pcm._confirm_tags)

    def test_get_confirm_deferreds(self):

        test_confirm_deferreds = {
                1: mock.Mock(),
                2: mock.Mock(),
                3: mock.Mock()
        }

        for publish_tag, deferred in test_confirm_deferreds.items():
            self.pcm.add_confirm_deferred(publish_tag, deferred)

        # single
        return_value = self.pcm.get_confirm_deferreds(publish_tag=2,
                multiple=False)
        self.assertEqual(len(return_value), 1)
        deferred, tag = return_value[0]
        self.assertIs(deferred, test_confirm_deferreds[2])
        self.assertEqual(tag, 2)

        # multiple
        expected_return_value = [
                (test_confirm_deferreds[1], 1),
                (test_confirm_deferreds[2], 2),
        ]
        return_value = self.pcm.get_confirm_deferreds(publish_tag=2,
                multiple=True)
        self.assertItemsEqual(return_value, expected_return_value)


    def test_private_fire_confirm_deferreds(self):
        self.pcm.get_confirm_deferreds = mock.Mock()

        fake_deferred = mock.Mock()
        fake_deferred.callback = mock.Mock()
        fake_tag = mock.Mock()
        # same format as get_confirm_deferred returns
        confirm_deferreds = [(fake_deferred, fake_tag)]
        self.pcm.get_confirm_deferreds.return_value = confirm_deferreds

        self.pcm.remove_confirm_deferred = mock.Mock()

        multiple = object()
        self.pcm._fire_confirm_deferreds(publish_tag=fake_tag,
                multiple=multiple)
        self.pcm.get_confirm_deferreds.assert_called_once_with(
                publish_tag=fake_tag, multiple=multiple)
        fake_deferred.callback.assert_called_once_with(fake_tag)
        self.pcm.remove_confirm_deferred.assert_called_once_with(fake_tag)
