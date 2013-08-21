from flow.brokers.amqp.channel_facade import ChannelFacade
from flow.protocol.exceptions import InvalidMessageException
from flow import interfaces
from injector import inject
from twisted.internet import defer
from flow.util.exit import exit_process
from flow.exit_codes import EXECUTE_SYSTEM_FAILURE


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

    def declare_queue(self, *args, **kwargs):
        return self.channel.declare_queue(*args, **kwargs)

    def register_handler(self, handler):
        LOG.debug("Registering handler on queue '%s'.", handler.queue_name)
        connect_deferred = self.channel.connect()
        connect_deferred.addCallback(self._start_handler, handler=handler)
        connect_deferred.addErrback(self._exit, handler=handler)
        return connect_deferred

    def _start_handler(self, _callback_arg, handler):
        queue_name = handler.queue_name
        consume_deferred = self.channel.basic_consume(queue=queue_name)
        consume_deferred.addCallback(self._begin_get_loop, handler=handler)
        consume_deferred.addErrback(self._exit, handler=handler)
        return _callback_arg

    def _begin_get_loop(self, _callback_arg, handler):
        queue, consumer_tag = _callback_arg
        queue_name = handler.queue_name
        LOG.debug('Beginning consumption on queue (%s)', queue_name)

        self._get_message_from_queue(queue=queue, handler=handler)
        return _callback_arg

    def _get_message_from_queue(self, queue, handler):
        deferred = queue.get()
        deferred.addCallback(self._message_recieved, queue=queue, handler=handler)
        deferred.addErrback(self._exit, handler=handler)

    def _message_recieved(self, _callback_arg, queue, handler):
        (channel, basic_deliver, properties, encoded_message) = _callback_arg

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
        deferred.addErrback(self._exit)

        self._get_message_from_queue(queue, handler)
        return _callback_arg

    def _exit(self, error, **kwargs):
        LOG.critical("Unexpected error with kwargs: %s\n%s", kwargs,
                error.getTraceback())
        exit_process(EXECUTE_SYSTEM_FAILURE)

    def _ack(self, _callback_arg, receive_tag):
        LOG.debug('Acking message (%s)', receive_tag)
        self.channel.basic_ack(receive_tag)
        return _callback_arg

    def _reject(self, error, receive_tag):
        LOG.error('Rejecting message (%s) due to error: %s', receive_tag,
                error.getTraceback())
        self.channel.basic_reject(receive_tag)
        return None # we don't want to engage additional errbacks
