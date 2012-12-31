import logging

from flow.job import Job

LOG = logging.getLogger(__name__)

class SuccessfulCompletionService(object):
    def __init__(self, redis=None, publish_function=None):
        self.redis = redis
        self.publish_function = publish_function

#        self.publish_function = functools.partial(exchange_manager.publish,
#                **basic_publish_properties)

    def message_handler(self, properties, input_data,
            ack_callback, reject_callback)

        job = Job.get(self.redis, key=job_key)
        jobs_to_start = job.complete()
        for jts in jobs_to_start:
            jts.start(publish_function=self.publish_function)

        ack_callback()

    def bad_data_handler(self, properties, body, ack_callback, reject_callback):
        LOG.debug('Got bad data, properties = %s: %s', properties, body)
        reject_callback()


class UnsuccessfulCompletionService(object):
    def __init__(self, redis=None):
        self.redis = redis

    def message_handler(self, properties, input_data,
            ack_callback, reject_callback)

        job = Job.get(self.redis, key=job_key)
        job.fail()

        ack_callback()

    def bad_data_handler(self, properties, body, ack_callback, reject_callback):
        LOG.debug('Got bad data, properties = %s: %s', properties, body)
        reject_callback()
