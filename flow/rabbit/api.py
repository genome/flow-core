from flow.configuration.settings.injector import setting

import injector
import json
import re
import requests


@injector.inject(bindings=setting('bindings'),
        hostname=setting('amqp.hostname'),
        port=setting('amqp.api_port'), virtual_host=setting('amqp.vhost'))
class RabbitMQAPI(object):
    @property
    def auth(self):
        return ('guest', 'guest')

    @property
    def parameters(self):
        return {
            'hostname': self.hostname,
            'port': self.port,
            'virtual_host': self.virtual_host,
        }

    def request_string(self, api_substring):
        template = 'http://%(hostname)s:%(port)s/api/' + api_substring
        return template % self.parameters


    def vhost_status(self):
        response = requests.get(self.request_string('vhosts/%(virtual_host)s'),
                auth=self.auth)
        return json.loads(response.text)

    def queue_info(self, queue_name):
        response = requests.get(self.request_string(
            'queues/%(virtual_host)s/' + queue_name), auth=self.auth)
        return json.loads(response.text)

    def queue_contents(self, queue_name, count, requeue):
        response = requests.post(self.request_string(
            'queues/%(virtual_host)s/' + queue_name + '/get'),
            data=json.dumps({
                'count': count,
                'encoding': 'auto',
                'requeue': requeue,
            }), auth=self.auth)
        return json.loads(response.text)

    def queue_show(self, queue_name_regex, queue_info_filter):
        rows = []
        for queue_name in self.queue_names_matching(queue_name_regex):
            rows.append(queue_info_filter(self.queue_info(queue_name)))

        return rows

    def queue_show_all(self, queue_name_regex):
        return [self.queue_info(q)
                for q in self.queue_names_matching(queue_name_regex)]

    def queue_get(self, regex, count, requeue, full):
        remaining_count = count
        results = {}
        for queue_name in self.queue_names_matching(regex):
            queue_results = self.queue_contents(queue_name,
                    count=remaining_count, requeue=requeue)
            remaining_count -= len(queue_results)
            if full:
                results[queue_name] = queue_results
            else:
                results[queue_name] = [qr['payload'] for qr in queue_results]
            if remaining_count < 1:
                break

        return results

    def queue_names_matching(self, pattern):
        regex = re.compile(pattern)
        for name in self.queue_names:
            if regex.match(name):
                yield name

    def publish_to_queue(self, queue_name, payload, payload_encoding='string',
            message_properties={}):
        response = requests.post(self.request_string(
            'exchanges/%(virtual_host)s/amq.default/publish'),
            data=json.dumps({
                'properties': message_properties,
                'payload': payload,
                'routing_key': queue_name,
                'payload_encoding': payload_encoding,
            }), auth=self.auth)
        if 200 != response.status_code:
            raise RuntimeError('%s -- failed to publish message to "%s" '
                    '(properties=%s): %s'
                    % (response, queue_name, message_properties, payload))

    @property
    def queue_names(self):
        results = ['missing_routing_key']
        results.extend(self.base_queue_names)
        results.extend(['dead_%s' % q for q in self.base_queue_names])
        return results

    @property
    def base_queue_names(self):
        return self.bindings['flow'].keys()
