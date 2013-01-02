import json
import logging
import pkg_resources
from flow.protocol import exceptions

LOG = logging.getLogger(__name__)


MESSAGE_CLASSES = {}

for ep in pkg_resources.iter_entry_points('flow.protocol.message_classes'):
    try:
        cls = ep.load()
        MESSAGE_CLASSES[cls.__name__] = cls
    except AttributeError:
        LOG.exception('Could not get name for class (%s)!', ep)


def encode(message):
    return json.dumps(message.to_dict())

def decode(encoded_message):
    try:
        d = json.loads(encoded_message)
    except:
        raise exceptions.InvalidMessageException(
                'Could not deserialized message: %s.' % encoded_message)

    try:
        message_class = d.pop('message_class')
    except KeyError:
        raise exceptions.InvalidMessageException(
                'No class name defined in message: %s' % encoded_message)

    try:
        cls = MESSAGE_CLASSES[message_class]
    except KeyError:
        raise exceptions.InvalidMessageException(
                "No message class registered with class name = '%s'"
                % message_class)

    return cls(**d)
