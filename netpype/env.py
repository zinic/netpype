import logging

from os import environ as env


_DEFAULT_LOG_LEVEL = logging.WARN
_CONSOLE_STREAM = logging.StreamHandler()

_VALID_LOGGING_LEVELS = [
    'DEBUG',
    'INFO',
    'ERROR',
    'WARN'
]


def _init():
    if not '_INITIALIZED' in globals():
        globals()['_INITIALIZED'] = True
        _set_defaults()

        log_level = get_env('LOG', None)
        if log_level:
            if log_level in _VALID_LOGGING_LEVELS:
                print('Setting logging level to {}.'.format(log_level))
                _DEFAULT_LOG_LEVEL = getattr(logging, log_level)
            else:
                print('Logging level {} not understood.'.format(log_level))


def get_logger(logger_name, level=_DEFAULT_LOG_LEVEL, propagate=False):
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.propagate = propagate
    return logger


def to_console(logger):
    logger.addHandler(_CONSOLE_STREAM)


def _set_log_defaults(logger_name):
    to_console(get_logger(logger_name))


def get_env(name, default=None):
    value = env.get(name)
    return value if value else default


def _set_defaults():
    map(_set_log_defaults, (
        'netpype',
        'netpype.selector',
        'netpype.server',
        'netpype.server.epoll',
        'netpype.server.poll',
        'netpype.channel'))

_init()
