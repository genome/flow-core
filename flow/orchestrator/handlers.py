import logging

from flow.orchestrator.redisom import get_object

LOG = logging.getLogger(__name__)

class OrchestratorNodeHandler(object):
    def __init__(self, redis=None, services=None, callback_name=None):
        self.redis = redis
        self.services = services
        self.callback_name = callback_name


    def message_handler(self, message):
        # Exceptions need to be propagated back up one level
        try:
            node = get_object(self.redis, message.return_identifier)
        except:
            try:
                LOG.exception('Failed to get node from return_identifier:',
                        message.return_identifier)
            except AttributeError:
                LOG.exception(
                        'Failed to get node from return_identifier (unknown)')
            finally:
                raise

        try:
            callback = getattr(node, self.callback_name)
        except AttributeError:
            LOG.exception('Failed to get node callback')
            raise

        try:
            callback(self.services, **message.to_dict())
        except NodeAlreadyCompletedError:
            LOG.exception('%s got node already completed error',
                    self.__class__.__name__)
        except NodeAlreadyExecutedError:
            LOG.exception('%s got node already executed error',
                    self.__class__.__name__)
        except RuntimeError:
            LOG.exception('Failed to execute node callback')
            raise
