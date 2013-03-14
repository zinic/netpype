import logging
import netpype.epoll as epoll
from netpype.channel import NetworkEventHandler, PipelineFactory

_LOG = logging.getLogger('netpype.simple')


class BasicHandler(NetworkEventHandler):

    def on_connect(self, message):
        _LOG.info('Connected to {}.'.format(message))
        return (epoll.REQUEST_READ, None)

    def on_read(self, message):
        _LOG.info('Read {} bytes as:\n{}'.format(len(message), message))
        return (epoll.REQUEST_WRITE, b'HTTP/1.1 200 OK\r\n\r\n')

    def on_write(self, message):
        return (epoll.REQUEST_CLOSE, None)

    def on_close(self, message):
        _LOG.info('Closing connection to {}'.format(message))


class BasicPipelineFactory(PipelineFactory):

    def upstream_pipeline(self):
        return [BasicHandler()]

    def downstream_pipeline(self):
        return [BasicHandler()]
