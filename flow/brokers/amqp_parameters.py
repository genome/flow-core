from injector import inject, Setting

@inject(hostname=Setting('amqp.hostname'), port=Setting('amqp.port'),
        virtual_host=Setting('amqp.vhost'))
class AmqpConnectionParameters(object): pass
