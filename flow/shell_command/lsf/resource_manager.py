from flow.configuration.settings.injector import setting
from flow.shell_command import factory
from flow.shell_command import resource_types
from flow.shell_command.lsf import resource

import injector


@injector.inject(
    resource_type_definitions=setting('shell_command.resource_types'),
    resource_definitions=setting('shell_command.lsf.supported_resources'))
class LSFResourceManager(object):
    def __init__(self):
        self.resource_types = resource_types.make_resource_types(
                self.resource_type_definitions)
        self.available_resources = {}
        self.available_resources['limit'] = factory.build_objects(
                self.resource_definitions.get('limit', {}), resource)
        self.available_resources['request'] = factory.build_objects(
                self.resource_definitions.get('request', {}), resource)
        self.available_resources['reserve'] = factory.build_objects(
                self.resource_definitions.get('reserve', {}), resource)

    def set_resources(self, request, executor_data):
        resources = resource_types.make_all_resource_objects(
                executor_data.get('resources', {}), self.resource_types)
        resource.set_all_resources(request, resources, self.available_resources)
