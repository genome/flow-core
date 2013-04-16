from injector import inject, Setting

@inject(
    connection_attempts=Setting('amqp.connection_attempts'),
    hostname=Setting('amqp.hostname'),
    port=Setting('amqp.port'),
    retry_delay=Setting('amqp.retry_delay'),
    socket_timeout=Setting('amqp.socket_timeout'),
    virtual_host=Setting('amqp.vhost'),
)
class AmqpConnectionParameters(object): pass
