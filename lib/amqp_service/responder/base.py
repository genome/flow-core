import json
import logging

LOG = logging.getLogger(__name__)


class Responder(object):
    def __init__(self, queue=None, exchange=None):
        self.queue = queue
        self.exchange = exchange

    def _on_message_wrapper(self, channel, basic_deliver, properties, body):
        LOG.debug('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)

        try:
            input_data = json.loads(body)

            routing_key, output_data = self.on_message(
                    channel, basic_deliver, properties, input_data)

            output_message = json.dumps(output_data)
            channel.basic_publish(exchange=self.exchange, body=output_message,
                    routing_key=routing_key)

            channel.basic_ack(basic_deliver.delivery_tag)

        except Exception as e:
            LOG.error("Rejecting message (%s): '%s'",
                    basic_deliver.delivery_tag, body)
            LOG.exception(e)
            channel.basic_reject(basic_deliver.delivery_tag, requeue=False)


    def on_message(self, channel, basic_deliver, properties, body):
        LOG.error('Attempted to receive message in base Responder class')
        raise RuntimeError(
                "on_message not defined for responder (%s)" % self.__class__)
