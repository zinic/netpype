import logging
from netpype import epoll_x as epoll

from netpype.channel import *

_LOG = logging.getLogger('netpype.simple')


class BasicEPollHandler(object):

    def on_connect(self, address):
        _LOG.info('Connected to {}.'.format(address))
        return (REQUEST_READ,)

    def on_read(self, channel):
        data = channel.recv(1024)
        _LOG.info('Read {} bytes as:\n{}'.format(len(data), data))
        return (REQUEST_WRITE,)

    def on_write(self, channel):
        channel.send(b'HTTP/1.1 200 OK\r\n\r\n')
        return (REQUEST_CLOSE,)

    def on_close(self, address):
        _LOG.info('Closing connection to {}'.format(address))


class PipelineFactory(object):

    def new_upstream_pipeline(self):
        return [BasicEPollHandler()]

    def new_downstream_pipeline(self):
        return [BasicEPollHandler()]

