from flow.configuration.settings.injector import setting
from injector import inject

@inject(
    connection_attempts=setting('amqp.connection_attempts'),
    hostname=setting('amqp.hostname'),
    port=setting('amqp.port'),
    retry_delay=setting('amqp.retry_delay'),
    socket_timeout=setting('amqp.socket_timeout'),
    virtual_host=setting('amqp.vhost'),
)
class AmqpConnectionParameters(object): pass
