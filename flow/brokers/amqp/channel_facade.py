from injector import inject
from twisted.internet import defer
from flow.brokers.amqp.connection_manager import ConnectionManager
from flow.brokers.amqp.publisher_confirm_manager import PublisherConfirmManager

import logging
import pika


LOG = logging.getLogger(__name__)


@inject(connection_manager=ConnectionManager)
class ChannelFacade(object):
    def __init__(self):
        self._publish_properties = pika.BasicProperties(delivery_mode=2)

        self._pika_channel = None
        self._publisher_confirm_manager = None
        self._last_publish_tag = 0

    def connect(self):
        connect_deferred = self.connection_manager.connect()

        # only do _on_connected once
        if not connect_deferred.called:
            connect_deferred.addCallback(self._on_connected)
        return connect_deferred

    def _on_connected(self, pika_channel):
        self._pika_channel = pika_channel
        self._publisher_confirm_manager = PublisherConfirmManager(
                self._pika_channel)
        return pika_channel

    def bind_queue(self, queue_name, exchange_name, topic, **properties):
        return self._connect_and_do('queue_bind', queue=queue_name,
                exchange=exchange_name, routing_key=topic, **properties)

    def declare_queue(self, queue_name, durable=True, **other_properties):
        return self._connect_and_do('queue_declare', queue=queue_name,
                durable=durable, **other_properties)

    def declare_exchange(self, exchange_name, exchange_type='topic',
            durable=True, **other_properties):
        return self._connect_and_do('exchange_declare', exchange=exchange_name,
                durable=durable, exchange_type=exchange_type,
                **other_properties)

    def basic_publish(self, exchange_name, routing_key, encoded_message):
        self._connect_and_do('basic_publish',
                exchange=exchange_name,
                routing_key=routing_key,
                body=encoded_message,
                properties=self._publish_properties)

        self._last_publish_tag += 1
        deferred = defer.Deferred()
        self._publisher_confirm_manager.add_confirm_deferred(
                publish_tag=self._last_publish_tag,
                deferred=deferred)
        return deferred

    def basic_ack(self, recieve_tag):
        return self._pika_channel.basic_ack(recieve_tag)

    def basic_reject(self, recieve_tag, requeue=False):
        return self._pika_channel.basic_reject(recieve_tag, requeue=requeue)

    def _connect_and_do(self, fn_name, *args, **kwargs):
        if self._pika_channel is None:
            connect_deferred = self.connect()

            deferred = defer.Deferred()
            connect_deferred.addCallback(self._do_on_channel, fn_name=fn_name,
                    args=args, kwargs=kwargs, deferred=deferred)
            return deferred
        else:
            channel_fn = getattr(self._pika_channel, fn_name)
            return channel_fn(*args, **kwargs)

    @staticmethod
    def _do_on_channel(pika_channel, fn_name, args, kwargs, deferred):
        LOG.debug("Executing %s", fn_name)
        channel_fn = getattr(pika_channel, fn_name)
        this_things_deferred = channel_fn(*args, **kwargs)
        this_things_deferred.chainDeferred(deferred)
        return pika_channel
