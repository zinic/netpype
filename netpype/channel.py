import socket
import select
import logging

from copy import copy


_LOG = logging.getLogger('netpype.channel')
_EMPTY_BUFFER = b''

UNIX_SOCK = socket.AF_UNIX
IPv4_SOCK = socket.AF_INET
IPv6_SOCK = socket.AF_INET6


def server_socket(socket_inet_addr):
    ssock = socket.socket(socket_inet_addr.type, socket.SOCK_STREAM)
    ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ssock.bind((socket_inet_addr.address, socket_inet_addr.port))
    ssock.setblocking(0)
    ssock.listen(1)
    return ssock


class SocketAddress(object):

    def __init__(self, type, address, port):
        self.type = type
        self.address = address
        self.port = port

    def __repr__(self):
        return '{} socket {}:{}'.format(self.type,
                                        self.address,
                                        self.port)


class SocketINet4Address(SocketAddress):

    def __init__(self, address, port):
        super(SocketINet4Address, self).__init__(IPv4_SOCK, address, port)


class SocketINet6Address(SocketAddress):

    def __init__(self, address, port):
        super(SocketINet6Address, self).__init__(IPv6_SOCK, address, port)


class HandlerPipeline(object):

    def __init__(self, pipeline_factory):
        self.upstream = pipeline_factory.upstream_pipeline()
        self.downstream = pipeline_factory.downstream_pipeline()


class ChannelPipeline(object):

    def __init__(self, channel, pipeline, client_addr):
        self.channel = channel
        self.fileno = channel.fileno()
        self.client_addr = client_addr
        self.pipeline = pipeline
        self.write_buffer = ChannelBuffer()


"""
This is a simple read buffer idiom to prevent buffer manipulation operations
while reading data.
"""
class ChannelBuffer(object):

    def __init__(self, initial_buffer=b''):
        self.set_buffer(initial_buffer)

    def set_buffer(self, new_buffer):
        self._buffer = new_buffer
        self._position = 0
        self._size = len(new_buffer)

    def size(self):
        return self._size

    def has_data(self):
        return self._position < self._size

    def remaining(self):
        return self._buffer[self._position:]

    def sent(self, bytes_read):
        self._position += bytes_read


"""
A PipelineFactory is responsible for building the upstream and downstream
handlers and organize them into an upstream pipeline and a downstream pipeline.
New pipelines are created for new connections, meaning that each pipeline has a
1:1 ratio with the server's sockets.
"""
class PipelineFactory(object):

    def upstream_pipeline(self):
        raise NotImplementedError

    def downstream_pipeline(self):
        raise NotImplementedError


"""
A NetworkEventHandler is a pipeline object that will both send and recieve
network events. Direct extension of this class is not required but recommended
for the default return values of the pre-existing methods.

When a method is called on the NetworkEventHandler, there is the expectation
that the method will either return None or return a tuple containing an event
signal and, if present, a message payload.
"""
class NetworkEventHandler(object):

    """
    A NetworkEventHandler may receive an event describing that a network client
    has connected with the pipeline successfully. The message argument of this
    method represents the address of the client that is now connected.

    This message will be called for every handler regardless of their return
    values.
    """
    def on_connect(self, message):
        return REQUEST_CLOSE, None

    """
    A NetworkEventHandler may recieve an event describing that a network client
    has disconnected. This disconnect may happen at any time or due to an error.
    The message argument of this method represents the address of the client
    that was connected.

    A handler may forward an event to the following handler by returning using
    the netpype.selector.FORWARD signal. The argument is passed to the next
    handler as its message.

    A handler may request socket events. Socket events break out of the pipeline
    and are acted upon immediately.

    The following socket events are allowed:
        * netpype.selector.REQUEST_WRITE
        * netpype.selector.REQUEST_READ
        * netpype.selector.REQUEST_CLOSE
    """
    def on_close(self, message):
        return None

    """
    A NetworkEventHandler may recieve an event describing that a network client
    has sent the server data. The message argument of this method represents
    the message that was sent through the pipeline from the actor behind this
    handler. The preceeding actor may be the source, in which case the message
    will be a buffer containing the, otherwise the message may be of any type
    and should be interpreted by the handler.

    A handler may forward an event to the following handler by returning using
    the netpype.selector.FORWARD signal. The argument is passed to the next
    handler as its message.

    A handler may request socket events. Socket events break out of the pipeline
    and are acted upon immediately.

    The following socket events are allowed:
        * netpype.selector.REQUEST_WRITE
        * netpype.selector.REQUEST_READ
        * netpype.selector.REQUEST_CLOSE
    """
    def on_read(self, message):
        return REQUEST_CLOSE, None

    """
    A NetworkEventHandler may recieve an event describing that a network client
    has sent the server data. The message argument of this method represents
    the message that was sent down through the pipeline from the actor ahead of
    this handler. The last return value of the pipeline is considered the final
    result if each handler forwards to the next.

    A handler may forward an event to the following handler by returning using
    the netpype.selector.FORWARD signal. The argument is passed to the next
    handler as its message.

    A handler may request selector events. Selector events break out of the
    pipeline and are acted upon immediately.

    The following socket events are allowed:
        * netpype.selector.REQUEST_WRITE
        * netpype.selector.REQUEST_READ
        * netpype.selector.REQUEST_CLOSE
    """
    def on_write(self, message):
        return REQUEST_CLOSE, None
