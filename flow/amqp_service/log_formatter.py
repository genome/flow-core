import copy
import logging

from flow.amqp_service import colors

DEFAULT_LEVEL_COLORS = {
        logging.DEBUG:    ('debug',    {'foreground': colors.GREY}),
        logging.INFO:     ('info',     {'foreground': colors.GREEN}),
        logging.WARNING:  ('warning',  {'foreground': colors.YELLOW}),
        logging.ERROR:    ('error',    {'foreground': colors.RED}),
        logging.CRITICAL: ('critical', {'foreground': colors.BLACK,
                                        'background': colors.RED}),
        }
DEFAULT_NAME_COLOR     = {'foreground': colors.BLUE}
DEFAULT_FUNCTION_COLOR = {'foreground': colors.CYAN}

class ColorFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None,
            level_colors=DEFAULT_LEVEL_COLORS,
            name_color=DEFAULT_NAME_COLOR,
            function_color=DEFAULT_FUNCTION_COLOR):
        logging.Formatter.__init__(self, fmt=fmt, datefmt=datefmt)
        self.level_colors = level_colors
        self.name_color = name_color
        self.function_color = function_color

    def format(self, record):
        # Make a copy so we don't interfere with other formatters
        mutable_record = copy.copy(record)

        level_name, level_color_options = self.level_colors[record.levelno]
        mutable_record.levelname = colors.wrap(level_name,
                **level_color_options)

        mutable_record.name = colors.wrap(record.name,
                **self.name_color)
        mutable_record.funcName = colors.wrap(record.funcName,
                **self.function_color)

        return logging.Formatter.format(self, mutable_record)
