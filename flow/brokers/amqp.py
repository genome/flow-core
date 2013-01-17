import logging
from flow.protocol import codec
from flow.protocol.exceptions import InvalidMessageException

import flow.amqp_manager
import os

LOG = logging.getLogger(__name__)

class AmqpBroker(object):
    def __init__(self, exchange_name='workflow'):
        arguments = {'alternate-exchange': '%s.alt' % exchange_name}
        self.exchange_name = exchange_name
        self.exchange_manager = flow.amqp_manager.ExchangeManager(
                exchange_name, durable=True, persistent=True, **arguments)

        amqp_url = os.getenv('AMQP_URL')
        if not amqp_url:
            amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
            LOG.warning("No AMQP_URL found, using '%s' by default", amqp_url)
        self.amqp_url = amqp_url

        self.queue_managers = []

        self.ready = False
        self._on_ready_publishes = []

        self.exit_code = 0

    def register_temporary_handler(self, queue_name, handler, routing_key):
        listener = AmqpListener(delivery_callback=handler)

        qm = flow.amqp_manager.QueueManager(
                queue_name, message_handler=listener, durable=False,
                auto_delete=True, exclusive=True,
                bindings=[{'exchange': self.exchange_name,
                           'topic': routing_key}])
        self.queue_managers.append(qm)

    def register_handler(self, queue_name, handler):
        listener = AmqpListener(delivery_callback=handler)

        qm = flow.amqp_manager.QueueManager(queue_name,
                message_handler=listener, durable=True)
        self.queue_managers.append(qm)

    def register_binding(self, routing_key, queue_name):
        # XXX No need to do anything here yet
        pass

    def publish(self, routing_key, message):
        if self.ready:
            encoded_message = codec.encode(message)
            self.exchange_manager.publish(routing_key=routing_key,
                    message=encoded_message)
        else:
            self._on_ready_publishes.append((routing_key, message))

    def listen(self):
        self.ready = True

        delegates = [self.exchange_manager]
        delegates.extend(self.queue_managers)
        channel_manager = flow.amqp_manager.ChannelManager(delegates=delegates,
                prefetch_count=2)
        self.connection_manager = flow.amqp_manager.ConnectionManager(
                self.amqp_url, delegates=[channel_manager])

        for routing_key, message in self._on_ready_publishes:
            self.connection_manager.add_ready_callback(
                    lambda x, k=routing_key, m=message: self.publish(k, m))

        self.connection_manager.start()
        return self.exit_code

    def exit(self, exit_code):
        self.exit_code = exit_code
        self.connection_manager.stop()


class AmqpListener(object):
    def __init__(self, delivery_callback=None):
        self.delivery_callback = delivery_callback

    def __call__(self, properties, encoded_message,
            ack_callback, reject_callback):
        try:
            message = codec.decode(encoded_message)

            self.delivery_callback(message)
            ack_callback()
        except InvalidMessageException as e:
            LOG.exception('Invalid message.  Properties = %s, message = %s',
                    properties, encoded_message)
            reject_callback()
        except KeyboardInterrupt:
            raise
        except:
            LOG.exception('Unexpected error handling message.')
            reject_callback()
