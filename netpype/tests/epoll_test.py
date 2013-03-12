import time
import unittest
import logging
import netpype.epoll

import multiprocessing

import cprofile

console = logging.StreamHandler()

def to_console(logger):
    logger.setLevel(logging.WARN)
    logger.addHandler(console)
    logger.propagate = False

map(to_console,
    (logging.getLogger('netpype'),
     logging.getLogger('netpype.epoll'),
     logging.getLogger('netpype.tests.epoll_test')))

_LOG = logging.getLogger('netpype.tests.epoll_test')


class BasicEPollHandler(netpype.epoll.AbstractEPollHandler):

    def on_connect(self, event, manager):
        _LOG.info('Connected to {}.'.format(event.address))
        manager.request_read()

    def on_read(self, event, manager):
        data = event.read()
        manager.request_write()
        _LOG.info('Read {} bytes as:\n{}'.format(len(data), data))

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


class WhenHosting(unittest.TestCase):

    def test_things(self):
        socket_info = netpype.epoll.SocketDescriptor(netpype.epoll.IPv4_SOCK, '127.0.0.1', 8080)
        server = netpype.epoll.EPollServer(socket_info, PipelineFactory())
        server.start()
        time.sleep(100)


if __name__ == '__main__':
        socket_info = netpype.epoll.SocketDescriptor(netpype.epoll.IPv4_SOCK, '127.0.0.1', 8080)
        server = netpype.epoll.EPollServer(socket_info, PipelineFactory())
        server.start()
        time.sleep(100)
