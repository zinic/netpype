import select
import sys
import netpype.env as env

from netpype.server.poll import PollSelectorServer
from netpype.server.epoll import EPollSelectorServer


_LOG = env.get_logger('netpype.selector')
_USE_GENERIC = env.get('GENERIC', False)

def new_server(socket_addr, pipeline_factory):
    if not _USE_GENERIC:
        if sys.platform == "linux2" and getattr(select, 'epoll'):
            _LOG.info('Selecting EPoll implementation.')
            return EPollSelectorServer(socket_addr, pipeline_factory)
        elif sys.platform == 'darwin':
            pass
        elif sys.platform == 'win32' or sys.platform == 'cygwin':
            pass
    _LOG.info('Selecting generic Poll implementation.')
    return PollSelectorServer(socket_addr, pipeline_factory)
