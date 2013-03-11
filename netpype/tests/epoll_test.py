import time
import unittest
import logging
import netpype.epoll

import multiprocessing


console = logging.StreamHandler()

def to_console(logger):
    logger.setLevel(logging.DEBUG)
    logger.addHandler(console)

map(to_console,
    (logging.getLogger('netpype'),
     logging.getLogger('netpype.epoll'),
     logging.getLogger('netpype.tests.epoll_test')))

_LOG = logging.getLogger('netpype.tests.epoll_test')


def suite():
    suite = unittest.TestSuite()
    suite.addTest(WhenHosting())

    return suite


class BasicEPollHandler(netpype.epoll.AbstractEPollHandler):

    def on_connect(self, event, manager):
        _LOG.info('Connected to {}.'.format(event.address))
        manager.request_read()

    def on_read(self, event, manager):
        data = event.read()
        _LOG.info('Read {} bytes.'.format(len(data)))


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
    unittest.main()
