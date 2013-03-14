import logging


_CONSOLE_STREAM = logging.StreamHandler()


def get_logger(logger_name, level=logging.WARN, propagate=False):
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.propagate = propagate
    return logger


def to_console(logger):
    logger.addHandler(_CONSOLE_STREAM)


def _set_defaults(logger_name):
    to_console(get_logger(logger_name))


map(_set_defaults, (
    'netpype',
    'netpype.selector',
    'netpype.channel'))
