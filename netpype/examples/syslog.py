import time
import netpype.env as env

from netpype.selector import events as selection_events, new_server
from netpype.channel import SocketINet4Address, CyclicBuffer
from netpype.channel import NetworkEventHandler, PipelineFactory


_LOG = env.get_logger('netpype.examples.simple')
_MAX_BYTES = 536870912

_STATE_START = 0
_STATE_READ_OCTET = 1
_STATE_READ_PRI = 2


class SyslogLexer(NetworkEventHandler):

    def __init__(self):
        self._accumulator = CyclicBuffer(size_hint=1024)
        self._lookaside = CyclicBuffer(size_hint=1024)
        self._state = _STATE_START

    def on_connect(self, message):
        _LOG.info('Syslog client connected @ {}.'.format(message))
        self._state = _STATE_READ_OCTET
        return (selection_events.REQUEST_READ, None)

    def on_read(self, message):
        # Load into our accumulator
        self._accumulator.put(message, 0)

        if self._state == _STATE_READ_OCTET:
            octet_count = bytearray(9)
            read = self._accumulator.get_until(octet_count, 0, ' ')

    def on_write(self, message):
        _LOG.info('Requesting close.')
        return (selection_events.REQUEST_CLOSE, None)

    def on_close(self, message):
        _LOG.info('Closing connection to {}'.format(message))


class BasicPipelineFactory(PipelineFactory):

    def upstream_pipeline(self):
        return [BasicHandler()]

    def downstream_pipeline(self):
        return [BasicHandler()]


def go():
    socket_info = SocketINet4Address('127.0.0.1', 8080)
    server = new_server(socket_info, BasicPipelineFactory())
    server.start()
    time.sleep(10000)
    server.stop()


if __name__ == '__main__':
    go()
