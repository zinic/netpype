import socket
import select
import logging

from multiprocessing import reduction, Pool, Queue, Value, cpu_count
from multiprocessing.forking import close as f_close


_LOG = logging.getLogger('netpype.channel')


UNIX_SOCK = socket.AF_UNIX
IPv4_SOCK = socket.AF_INET
IPv6_SOCK = socket.AF_INET6

WRITE_AVAILABLE = 0
READ_AVAILABLE = 1
CHANNEL_CONNECTED = 2
CHANNEL_CLOSED = 3
INTEREST_REQUEST = 4
RECLAIM_CHANNEL = 5

REQUEST_WRITE = 100
REQUEST_READ = 101
REQUEST_CLOSE = 102
FORWARD = 103


def new_serversocket(socket_type, bind_address, bind_port):
    ssock = socket.socket(socket_type, socket.SOCK_STREAM)
    ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ssock.bind((bind_address, bind_port))
    ssock.setblocking(0)
    ssock.listen(1)
    return ssock


class SocketDescriptor(object):

    def __init__(self, socket_type, bind_address, bind_port):
        self.socket_type = socket_type
        self.bind_address = bind_address
        self.bind_port = bind_port

    def __repr__(self):
        return '{} socket {}:{}'.format(self.socket_type,
                                        self.bind_address,
                                        self.bind_port)

class HandlerPipelineFactory(object):

    def new_upstream_pipeline(self):
        raise NotImplementedError

    def new_downstream_pipeline(self):
        raise NotImplementedError


class PipelineEvent(object):

    def __init__(self, signal):
        self.signal = signal


class PipelineMessage(PipelineEvent):

    def __init__(self, signal, payload=None):
        super(PipelineMessage, self).__init__(signal)
        self.payload = payload


class ChannelInterestEvent(PipelineEvent):

    def __init__(self, signal, socket_fileno, interest=None):
        super(ChannelInterestEvent, self).__init__(signal)
        self.socket_fileno = socket_fileno
        self.interest = interest


class ChannelConnectedEvent(PipelineEvent):

    def  __init__(self, address):
        super(ChannelConnectedEvent, self).__init__(CHANNEL_CONNECTED)
        self.address = address


class ChannelClosedEvent(PipelineEvent):

    def  __init__(self, address):
        super(ChannelClosedEvent, self).__init__(CHANNEL_CLOSED)
        self.address = address


class ChannelEvent(PipelineEvent):

    def  __init__(self, signal, handle):
        super(ChannelEvent, self).__init__(signal)
        self._handle = handle


class ChannelReadEvent(ChannelEvent):

    def  __init__(self, handle):
        super(ChannelReadEvent, self).__init__(READ_AVAILABLE, handle)

    def read(self):
        return self._handle.get_channel().recv(2048)


class ChannelWriteEvent(ChannelEvent):

    def  __init__(self, handle):
        super(ChannelWriteEvent, self).__init__(WRITE_AVAILABLE, handle)

    def write(self, payload=b''):
        if len(payload) > 0:
            return self._handle.get_channel().send(payload)
        return 0


class ChannelPipeline(object):

    def __init__(self, pipeline_factory):
        self.upstream = pipeline_factory.new_upstream_pipeline()
        self.downstream = pipeline_factory.new_upstream_pipeline()


class PipelineDriverError(Exception):

    def __init__(self, msg):
        self.msg = msg


class ChannelHandle(object):

    def __init__(self, fileno, socket_type):
        self.fileno = fileno
        self.socket_type = socket_type
        self._channel = None
        self._handle = reduction.reduce_handle(fileno)

    def get_channel(self):
        if not self._channel:
            self._channel_fd = reduction.rebuild_handle(self._handle)
            self._channel = socket.fromfd(self._channel_fd, self.socket_type, socket.SOCK_STREAM)
            self._channel.setblocking(0)
        return self._channel

    def close(self):
        f_close(self._channel_fd)
        self._channel.close()


class ChannelDescriptor(object):

    def __init__(self, channel, address, pipeline):
        self.state = ChannelState()
        self.fileno = channel.fileno()
        self.channel = channel
        self.address = address
        self.pipeline = pipeline

    def new_handle(self):
        return ChannelHandle(self.fileno, self.channel.type)


class ChannelState(object):

    def __init__(self):
        self._processing = False
        self._last_event = -1

    def is_interested(self, event):
        return event != self._last_event

    def in_use(self):
        return self._processing

    def use(self, event):
        #_LOG.debug('Channel {} in use.'.format(self._fileno))
        self._processing = True
        self._last_event = event

    def release(self, arg=None):
        #_LOG.debug('Channel {} released.'.format(self._fileno))
        self._processing = False
