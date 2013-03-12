import logging
import netpype.epoll


_LOG = logging.getLogger('netpype.simple')


class BasicEPollHandler(netpype.epoll.AbstractEPollHandler):

    def on_connect(self, event, manager):
        _LOG.info('Connected to {}.'.format(event.address))
        manager.request_read()

    def on_read(self, event, manager):
        data = event.read()
        _LOG.info('Read {} bytes as:\n{}'.format(len(data), data))
        manager.request_write()

    def on_write(self, event, manager):
        event.write(b'HTTP/1.1 200 OK\r\n\r\n')
        manager.request_close()

    def on_close(self, event):
        _LOG.info('Closing')


class PipelineFactory(netpype.epoll.HandlerPipelineFactory):

    def new_upstream_pipeline(self):
        return [BasicEPollHandler()]

    def new_downstream_pipeline(self):
        return [BasicEPollHandler()]

