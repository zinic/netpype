import logging
import netpype.epoll

from netpype.channel import *

_LOG = logging.getLogger('netpype.simple')


class BasicEPollHandler(netpype.epoll.AbstractEPollHandler):

    def on_connect(self, event):
        _LOG.info('Connected to {}.'.format(event.address))
        return PipelineMessage(REQUEST_READ)

    def on_read(self, event):
        data = event.read()
        _LOG.info('Read {} bytes as:\n{}'.format(len(data), data))
        return PipelineMessage(REQUEST_WRITE)

    def on_write(self, event):
        event.write(b'HTTP/1.1 200 OK\r\n\r\n')
        return PipelineMessage(REQUEST_CLOSE)

    def on_close(self, event):
        _LOG.info('Closing')


class PipelineFactory(netpype.epoll.HandlerPipelineFactory):

    def new_upstream_pipeline(self):
        return [BasicEPollHandler()]

    def new_downstream_pipeline(self):
        return [BasicEPollHandler()]

