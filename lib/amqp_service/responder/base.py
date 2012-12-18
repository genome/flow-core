import json
import logging
import pika

LOG = logging.getLogger(__name__)


class Responder(object):
    def __init__(self, queue=None, durable_queue=True, prefetch_count=1,
            exchange=None, exchange_type='topic', alternate_exchange=None):
        self.queue = queue
        self.durable_queue = durable_queue
        self.prefetch_count = prefetch_count
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.alternate_exchange = alternate_exchange

        self._properties = pika.BasicProperties(delivery_mode=2)

    def message_receiver(self, channel, basic_deliver, properties, body):
        LOG.debug('Received message # %s from %s',
                    basic_deliver.delivery_tag, properties.app_id)

        try:
            input_data = json.loads(body)

            routing_key, output_data = self.on_message(
                    channel, basic_deliver, properties, input_data)

            output_message = json.dumps(output_data)
            LOG.debug("Publishing to routing_key (%s)", routing_key)
            channel.basic_publish(exchange=self.exchange, body=output_message,
                    routing_key=routing_key, properties=self._properties)

            channel.basic_ack(basic_deliver.delivery_tag)

        except Exception as e:
            LOG.error("Rejecting message due to %s (%s): '%s'",
                    e.__class__, basic_deliver.delivery_tag, body)
            LOG.exception(e)
        except:
            LOG.error("Rejecting message due to non-standard exception (%s): '%s'",
                    basic_deliver.delivery_tag, body)
            channel.basic_reject(basic_deliver.delivery_tag, requeue=False)

    def on_message(self, channel, basic_deliver, properties, body):
        LOG.error('Attempted to receive message in base Responder class')
        raise RuntimeError(
                "on_message not defined for responder (%s)" % self.__class__)
