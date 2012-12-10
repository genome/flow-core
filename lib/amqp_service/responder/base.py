import json
import logging

from amqp_service.exceptions import ResponderTaskFailed

LOG = logging.getLogger(__name__)


class Responder(object):
    def __init__(self, amqp_manager, queue=None, exchange=None,
            success_response_key=None, fail_response_key=None,
            error_response_key=None):
        self.amqp_manager = amqp_manager
        self.queue = queue
        self.exchange = exchange
        self.success_response_key = success_response_key
        self.fail_response_key = fail_response_key
        self.error_response_key = error_response_key

    def _on_message_wrapper(self, channel, basic_deliver, properties, body):
        LOG.debug('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)

        try:
            data = json.loads(body)
        except Exception as e:
            self._respond(self.on_error, e,
                    channel, basic_deliver, properties, body)

        try:
            success_data = self.on_message(
                    channel, basic_deliver, properties, data)
            success_message = json.dumps(success_data)
            self._respond(self.on_success, success_message,
                    channel, basic_deliver, properties, body)
        except ResponderTaskFailed as e:
            self._respond(self.on_failure, e,
                    channel, basic_deliver, properties, body)
        except Exception as e:
            self._respond(self.on_error, e,
                    channel, basic_deliver, properties, body)


    def _respond(self, handler, pass_through, channel, basic_deliver,
            properties, body):
        try:
            handler(pass_through, channel, basic_deliver, properties, body)
            channel.basic_ack(basic_deliver.delivery_tag)
        except Exception as e:
            channel.basic_reject(basic_deliver.delivery_tag, requeue=False)


    def on_message(self, channel, basic_deliver, properties, body):
        raise RuntimeError("on_message not defined for this Responder")

    def on_success(self, message, channel, basic_deliver, properties, body):
            channel.basic_publish(exchange=self.exchange, body=message,
                    routing_key=self.success_response_key)

    def on_failure(self, exception, channel, basic_deliver, properties, body):
        raise RuntimeError("on_failure not defined for this Responder")

    def on_error(self, exception, channel, basic_deliver, properties, body):
        raise RuntimeError("on_error not defined for this Responder")


    def run(self):
        raise RuntimeError("I still need to implement this")
        self.amqp_manager.subscribe(blah)
