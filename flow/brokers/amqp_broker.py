from flow.brokers.amqp.channel_facade import ChannelFacade
from flow.protocol.exceptions import InvalidMessageException
from flow import interfaces
from injector import inject
from twisted.internet import defer
from flow.util.exit import exit_process
from flow.exit_codes import EXECUTE_SYSTEM_FAILURE
from flow.util.defer import add_callback_and_default_errback


import logging


LOG = logging.getLogger(__name__)


@inject(channel=ChannelFacade)
class AmqpBroker(interfaces.IBroker):
    def publish(self, exchange_name, routing_key, message):
        LOG.debug("Publishing to exchange (%s) with routing_key (%s) "
                "the message (%s)", exchange_name, routing_key, message)

        encoded_message = message.encode()

        return self.channel.basic_publish(
                exchange_name=exchange_name,
                routing_key=routing_key,
                encoded_message=encoded_message)

    def register_handler(self, handler):
        LOG.debug("Registering handler on queue '%s'.", handler.queue_name)
        connect_deferred = self.channel.connect()
        add_callback_and_default_errback(connect_deferred, self._start_handler,
                handler)
        return connect_deferred

    def declare_queue(self, *args, **kwargs):
        return self.channel.declare_queue(*args, **kwargs)

    def _start_handler(self, _cb_channel, handler):
        queue_name = handler.queue_name
        consume_deferred = self.channel.basic_consume(queue=queue_name)
        add_callback_and_default_errback(consume_deferred, self._begin_get_loop,
                handler)
        return _cb_channel

    def _begin_get_loop(self, consume_info, handler):
        queue, consumer_tag = consume_info
        queue_name = handler.queue_name
        LOG.debug('Beginning consumption on queue (%s)', queue_name)

        self._get_message_from_queue(queue=queue, handler=handler)
        return consume_info

    def _get_message_from_queue(self, queue, handler):
        deferred = queue.get()
        deferred.addCallback(self._on_message_recieved, queue, handler)
        deferred.addErrback(self._on_get_failed, handler)
        return deferred

    def _on_get_failed(self, reason, handler):
        LOG.critical("Get from queue '%s' failed: %s", handler.queue_name,
                reason)
        exit_process(EXECUTE_SYSTEM_FAILURE)

    def _on_ack_reject_failed(self, error):
        LOG.critical('Failed to ack or reject:\n%s', error.getTrackback())
        exit_process(EXECUTE_SYSTEM_FAILURE)

    def _on_message_recieved(self, get_info, queue, handler):
        (channel, basic_deliver, properties, encoded_message) = get_info

        message_class = handler.message_class
        try:
            message = message_class.decode(encoded_message)
            deferred = handler(message)
        except InvalidMessageException as e:
            LOG.exception('Invalid message.  message = %s', encoded_message)
            deferred = defer.fail(e)

        receive_tag = basic_deliver.delivery_tag
        deferred.addCallbacks(self._ack, self._reject,
                callbackArgs=(receive_tag,),
                errbackArgs=(receive_tag,))
        deferred.addErrback(self._on_ack_reject_failed)

        self._get_message_from_queue(queue, handler)
        return get_info

    def _ack(self, confirm_info, receive_tag):
        LOG.debug('Acking message (%s)', receive_tag)
        self.channel.basic_ack(receive_tag)
        return confirm_info

    def _reject(self, reason, receive_tag):
        LOG.error('Rejecting message (%s) due to error: %s', receive_tag,
                reason.getTraceback())
        self.channel.basic_reject(receive_tag)
        return reason
