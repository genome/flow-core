from flow import exit_codes
from flow.util.exit import exit_process
from pika.spec import Basic

import blist
import logging
import os


LOG = logging.getLogger(__name__)


class PublisherConfirmManager(object):
    def __init__(self, channel):
        self._confirm_tags = blist.sortedlist()
        self._confirm_deferreds = {}

        LOG.debug('Enabling publisher confirms.')
        channel.confirm_delivery()
        channel.callbacks.add(channel.channel_number, Basic.Ack,
                self._on_publisher_confirm_ack, one_shot=False)
        channel.callbacks.add(channel.channel_number, Basic.Nack,
                self._on_publisher_confirm_nack, one_shot=False)

    def _on_publisher_confirm_ack(self, method_frame):
        publish_tag = method_frame.method.delivery_tag
        multiple = method_frame.method.multiple
        LOG.debug('Got publisher confirm for message (%d), multiple = %s',
                publish_tag, multiple)

        self._fire_confirm_deferreds(publish_tag=publish_tag, multiple=multiple)

    def _on_publisher_confirm_nack(self, method_frame):
        """
        This indicates very bad situations, we'll just die if this happens.
        """
        publish_tag = method_frame.method.delivery_tag
        multiple = method_frame.method.multiple
        LOG.critical('Got publisher rejection for message (%d), multiple = %s',
                publish_tag, multiple)
        exit_process(exit_codes.EXECUTE_SYSTEM_FAILURE)

    def _fire_confirm_deferreds(self, publish_tag, multiple):
        confirm_deferreds = self.get_confirm_deferreds(publish_tag=publish_tag,
                multiple=multiple)
        for deferred, tag in confirm_deferreds:
            deferred.callback(tag)
            self.remove_confirm_deferred(tag)

    def get_confirm_deferreds(self, publish_tag, multiple):
        if multiple:
            index = self._confirm_tags.bisect(publish_tag)
            tags = self._confirm_tags[:index]
            deferreds = [(self._confirm_deferreds[tag], tag) for tag in tags]
            return deferreds
        else:
            if publish_tag in self._confirm_deferreds:
                return [(self._confirm_deferreds[publish_tag], publish_tag)]
            else:
                return []

    def add_confirm_deferred(self, publish_tag, deferred):
        if publish_tag not in self._confirm_deferreds:
            self._confirm_tags.add(publish_tag)
            self._confirm_deferreds[publish_tag] = deferred

    def remove_confirm_deferred(self, publish_tag):
        self._confirm_tags.remove(publish_tag)
        del self._confirm_deferreds[publish_tag]


