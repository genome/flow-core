from flow.configuration.parser import create_parser

import pkg_resources


_FLOW_COMMAND_CATEGORY = 'flow.commands'


def determine_command():
    registered_command_names = load_command_names()
    parser = create_parser(registered_command_names)
    namespace, remaining_args = parser.parse_known_args()

    command = get_command_class(namespace.command)
    command.name = namespace.command

    return command


def load_command_names(category=_FLOW_COMMAND_CATEGORY):
    results = []
    for ep in pkg_resources.iter_entry_points(category):
        results.append(ep.name)

    return results


def get_command_class(name, category=_FLOW_COMMAND_CATEGORY):
    return pkg_resources.iter_entry_points(category, name).next().load()
