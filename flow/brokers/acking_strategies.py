from flow.util import stats
from pika.spec import Basic

import blist
import collections
import injector
import logging


LOG = logging.getLogger(__name__)


class TagRelationships(object):
    def __init__(self):
        self.reset()

    @property
    def stats(self):
        return {
            'ackable_receive_tags': len(self._ackable_receive_tags),
            'non_ackable_receive_tags': len(self._non_ackable_receive_tags),
            'unconfirmed_publish_tags': len(self._unconfirmed_publish_tags)
        }

    def reset(self):
        LOG.debug('Restting PublisherConfirmation state.')
        self._ackable_receive_tags = blist.sortedlist()
        self._non_ackable_receive_tags = blist.sortedlist()
        self._unconfirmed_publish_tags = blist.sortedlist()

        self._publish_to_receive_map = {}
        self._receive_to_publish_set_map = collections.defaultdict(set)

    def add_publish_tag(self, receive_tag=None, publish_tag=None):
        timer = stats.create_timer('pub_confirm.add_publish_tag')
        timer.start()
        if receive_tag in self._ackable_receive_tags:
            self._ackable_receive_tags.remove(receive_tag)
            self._non_ackable_receive_tags.add(receive_tag)

        self._receive_to_publish_set_map[receive_tag].add(publish_tag)
        self._publish_to_receive_map[publish_tag] = receive_tag
        self._unconfirmed_publish_tags.add(publish_tag)
        timer.stop()

    def add_receive_tag(self, receive_tag):
        self._ackable_receive_tags.add(receive_tag)

    def remove_publish_tag(self, publish_tag, multiple=False):
        timer = stats.create_timer('pub_confirm.remove_publish_tag')
        timer.start()
        if multiple:
            max_index = self._unconfirmed_publish_tags.bisect(publish_tag)
            timer.split('multiple_max_index_bisect')

            ready_tags = self._unconfirmed_publish_tags[:max_index]
            del self._unconfirmed_publish_tags[:max_index]
            timer.split('multiple_delete_tags')

            LOG.debug('Multiple confirm for (%d) includes: %s',
                    publish_tag, ready_tags)

            for tag in ready_tags:
                self.remove_single_publish_tag(tag)
            timer.split('multiple_remove_tags')
        else:
            LOG.debug('Single confirm for (%d)', publish_tag)
            self._unconfirmed_publish_tags.remove(publish_tag)
            self.remove_single_publish_tag(publish_tag)
        timer.stop()

    def remove_receive_tag(self, receive_tag):
        LOG.debug('Removing receive tag (%d)', receive_tag)
        self._ackable_receive_tags.discard(receive_tag)
        self._non_ackable_receive_tags.discard(receive_tag)
        # Just throw away these mappings
        self._receive_to_publish_set_map.pop(receive_tag, None)

    def remove_single_publish_tag(self, publish_tag):
        receive_tag = self._publish_to_receive_map.pop(publish_tag, None)
        if receive_tag in self._receive_to_publish_set_map:
            LOG.debug('Publish tag (%d) maps to receive tag (%d)',
                    publish_tag, receive_tag)
            self._normal_confirm(publish_tag, receive_tag)
        else:
            LOG.debug('Publish tag (%d) maps to removed receive tag (%d)',
                    publish_tag, receive_tag)

    def _normal_confirm(self, publish_tag, receive_tag):
        publish_tag_set = self._receive_to_publish_set_map[receive_tag]
        publish_tag_set.remove(publish_tag)

        if not publish_tag_set:
            del self._receive_to_publish_set_map[receive_tag]
            self._set_receive_tag_ready_to_ack(receive_tag)
            LOG.debug('Receive tag (%d) ready to ack', receive_tag)
        else:
            LOG.debug('Waiting for %d more publisher confirms '
                    'before acking received message (%d)',
                    len(publish_tag_set), receive_tag)

    def _set_receive_tag_ready_to_ack(self, receive_tag):
        self._non_ackable_receive_tags.remove(receive_tag)
        self._ackable_receive_tags.add(receive_tag)


    def pop_ackable_receive_tags(self):
        '''
        Returns 2 element tuple.
            First element is a sorted list of receive_tags to ack.
            Second element is whether the first (smallest) tag should be multiple-ack'd
        '''
        # Cases
        # -----
        # 1) There are no ackable tags:
        #   rv = ([], False)
        # 2) There are no unackable tags:
        #   rv = ([ackable_tags[-1]], True)
        # 3) There are unackable tags
        # OR 4) The ackable tags are all less than the smallest unackable tag
        #   rv = ([ackable_tags[-1]], True)
        # 5) All the ackable tags are greater than the smallest unackable tag
        # OR 6) The smallest unackable tag is in the middle of the ackable tags
        #   This is the complex case:
        #       first ackable tag is the largest one smaller than the
        #       smallest unackable tag, rest are just thrown in the list
        #       if there is more than one tag smaller than smallest
        #       unackable, second rv element = True

        ackable_tags = self._ackable_receive_tags
        LOG.debug('Effective ack size = %d', len(ackable_tags))

        if not ackable_tags:
            return [], False

        timer = stats.create_timer('pub_confirm.pop_ackable_receive_tags')
        timer.start()

        unackable_tags = self._non_ackable_receive_tags
        if (not unackable_tags) or (unackable_tags[0] > ackable_tags[-1]):
            LOG.debug('Ackable tags are smaller than smallest unackable tag')
            ready_tags = [ackable_tags[-1]]
            multiple = len(ackable_tags) > 1
        else:
            # We assume that a tag is never in both ackable and unackable lists
            index = ackable_tags.bisect(unackable_tags[0])
            LOG.debug('Smallest unackable tag (%d) would insert at index %d',
                    unackable_tags[0], index)
            ready_tags = []
            multiple = False
            if index:
                ready_tags.append(ackable_tags[index - 1])
                if index > 1:
                    multiple = True
            ready_tags.extend(ackable_tags[index:])

        self._ackable_receive_tags = blist.sortedlist()

        timer.stop()
        return ready_tags, multiple

@injector.inject(_tag_relationships=TagRelationships)
class PublisherConfirmation(object):
    def reset(self):
        self._tag_relationships.reset()

    def register_broker(self, broker):
        self.broker = broker

    def on_channel_open(self, channel):
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

        self.remove_publish_tag(publish_tag, multiple=multiple)
        self.broker.ack_if_able()

    def _on_publisher_confirm_nack(self, _):
        LOG.critical('Got failed publisher confirm.  Killing broker.')
        self.broker.disconnect()


    def add_publish_tag(self, *args, **kwargs):
        return self._tag_relationships.add_publish_tag(*args, **kwargs)

    def add_receive_tag(self, *args, **kwargs):
        return self._tag_relationships.add_receive_tag(*args, **kwargs)

    def remove_publish_tag(self, *args, **kwargs):
        return self._tag_relationships.remove_publish_tag(*args, **kwargs)

    def remove_receive_tag(self, *args, **kwargs):
        return self._tag_relationships.remove_receive_tag(*args, **kwargs)

    def pop_ackable_receive_tags(self):
        return self._tag_relationships.pop_ackable_receive_tags()
