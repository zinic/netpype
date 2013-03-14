import time
import unittest
import logging
import multiprocessing

import netpype.simple
import netpype.epoll as epoll

from netpype.channel import SocketINet4Address



_LOG = logging.getLogger('netpype.tests.epoll_test')
console = logging.StreamHandler()


def to_console(logger):
    logger.setLevel(logging.WARN)
    logger.addHandler(console)
    logger.propagate = False


map(to_console,
    (logging.getLogger('netpype'),
     logging.getLogger('netpype.epoll'),
     logging.getLogger('netpype.simple'),
     logging.getLogger('netpype.tests.epoll_test')))


class WhenHosting(unittest.TestCase):

    def test_things(self):
        socket_info = SocketINet4Address('127.0.0.1', 8080)
        server = epoll.EPollServer(
            socket_info, netpype.simple.PipelineFactory())
        server.start()
        time.sleep(100)


def go():
    socket_info = SocketINet4Address('127.0.0.1', 8080)
    server = epoll.EPollServer(
        socket_info,netpype.simple.BasicPipelineFactory())
    server.start()
    time.sleep(10000)
    server.stop()

if __name__ == '__main__':
    go()
