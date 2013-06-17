import argparse


def create_parser(valid_command_names):
    parser = argparse.ArgumentParser()

    parser.add_argument('command', choices=valid_command_names)

    return parser


def parse_arguments(command_class):
    parser = create_parser([command_class.name])
    command_class.annotate_parser(parser)

    return parser.parse_args()
