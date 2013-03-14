import logging
import select
import sys

from netpype.server.poll import PollSelectorServer
from netpype.server.epoll import EPollSelectorServer


_LOG = logging.getLogger('netpype.selector.server')


def new_server(socket_addr, pipeline_factory):
    if sys.platform == "linux2" and getattr(select, 'epoll'):
        return EPollSelectorServer(socket_addr, pipeline_factory)
    elif sys.platform == 'darwin':
        pass
    elif sys.platform == 'win32' or sys.platform == 'cygwin':
        pass
    return PollSelectorServer(socket_addr, pipeline_factory)
