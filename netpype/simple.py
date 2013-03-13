import logging
from netpype import epoll_s as epoll

_LOG = logging.getLogger('netpype.simple')


class BasicEPollHandler(object):

    def on_connect(self, address):
        _LOG.info('Connected to {}.'.format(address))
        return (epoll.REQUEST_READ, None)

    def on_read(self, data):
        _LOG.info('Read {} bytes as:\n{}'.format(len(data), data))
        return (epoll.REQUEST_WRITE, b'HTTP/1.1 200 OK\r\n\r\n')

    def on_write(self, channel):
        return (epoll.REQUEST_CLOSE, None)

    def on_close(self, address):
        _LOG.info('Closing connection to {}'.format(address))


class PipelineFactory(object):

    def new_upstream_pipeline(self):
        return [BasicEPollHandler()]

    def new_downstream_pipeline(self):
        return [BasicEPollHandler()]
