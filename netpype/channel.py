import socket
import select
import netpype.env as env

_LOG = env.get_logger('netpype.channel')

try:
    from netpype.cutil import buffer_seek as seek
except ImportError:
    _LOG.warn('Unable to find C extensions. Falling back on python impl.')
    def seek(delim, source, size, read_index, available):
        seek_offset = 0
        while seek_offset < available:
            seek_index = read_index + seek_offset
            if seek_index + seek_offset >= size:
                seek_index -= size
            if source[seek_index] == delim:
                return seek_offset
            seek_offset += 1
        return -1

_EMPTY_BUFFER = bytearray()

UNIX_SOCK = socket.AF_UNIX
IPv4_SOCK = socket.AF_INET
IPv6_SOCK = socket.AF_INET6


def server_socket(socket_inet_addr):
    ssock = socket.socket(socket_inet_addr.type, socket.SOCK_STREAM)
    ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ssock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    ssock.bind((socket_inet_addr.address, socket_inet_addr.port))
    ssock.setblocking(0)
    ssock.listen(100)
    return ssock


class SocketAddress(object):

    def __init__(self, type, address, port):
        self.type = type
        self.address = address
        self.port = port

    def __repr__(self):
        return '{} socket {}:{}'.format(self.type, self.address, self.port)


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


def array_copy(source, src_offset, destination, dest_offset, length):
    destination[dest_offset:dest_offset+length] = source[src_offset:src_offset+length]


class CyclicBuffer(object):
    
    def __init__(self, size_hint=4096, data=None):
        if data:
            data_size = len(data)
            buffer_size = data_size if data_size > size_hint else size_hint
            self._buffer = bytearray(buffer_size)
            self._current_size = buffer_size
            self.clear()
            self.put(data, 0, data_size)
        else:
            self._buffer = bytearray(size_hint)
            self._current_size = size_hint
            self.clear()

    def skip_until(self, delim, limit=-1):
        seek_offset = seek(delim, self._buffer, 
            self._current_size, self._read_index, self._available)
        if seek_offset > 0:
            return self.skip(seek_offset)
        return seek_offset

    def get_until(self, delim, data, offset=0, limit=-1):
        seek_offset = seek(delim, self._buffer, 
            self._current_size, self._read_index, self._available)
        if seek_offset > 0:
            return self.get(data, offset, seek_offset)
        return seek_offset

    def get(self, data, offset=0, length=None):
        if length is None or length > self._available:
            readable = self._available
        else:
            readable = length

        if self._available > 0:
            if self._read_index + readable >= self._current_size:
                trimmed_length = self._current_size - self._read_index
                next_read_index = readable - trimmed_length
                array_copy(self._buffer, self._read_index, data,
                           offset, trimmed_length)
                array_copy(self._buffer, 0, data,
                           offset + trimmed_length, next_read_index)
                self._read_index = next_read_index
            else:
                array_copy(self._buffer, self._read_index,
                           data, offset, readable)
                if self._read_index + readable < self._current_size:
                    self._read_index += readable
                else:
                    self._read_index = readable - (
                        self._current_size - self._read_index)
            self._available -= readable
        return readable

    def put(self, data, offset=0, length=None):
        if length is None:
            length = len(data)
        remaining = self._current_size - self._available
        if remaining < length:
            self.grow(length - remaining)
        if self._write_index + length >= self._current_size:
            trimmed_length = self._current_size - self._write_index
            next_write_index = length - trimmed_length
            array_copy(data, offset, self._buffer,
                       self._write_index, trimmed_length)
            array_copy(data, offset + trimmed_length,
                       self._buffer, 0, next_write_index)
            self._write_index = next_write_index
        else:
            array_copy(data, offset, self._buffer,
                       self._write_index, length)
            self._write_index += length
        self._available += length
        return length

    def skip(self, length):
        bytes_skipped = 0
        if self._available > 0:
            if length > self._available:
                bytes_skipped = self._available
                self._read_index = 0
                self._write_index = 0
            else:
                bytes_skipped = length
                if self._read_index + length < self._current_size:
                    self._read_index += length
                else:
                    self._read_index = length - self._current_size
                    self._read_index -= self._read_index
            self._available -= bytes_skipped
        return bytes_skipped

    def grow(self, min_length):
        new_size = self._current_size * 2 * (
            int(min_length / self._current_size) + 1)
        new_buffer = bytearray(new_size)
        read = self.get(new_buffer, 0, new_size)
        self._buffer = new_buffer
        self._current_size = new_size
        self._read_index = 0
        self._write_index = read

    def available(self):
        return self._available

    def remaining(self):
        return self._current_size - self._available

    def __repr__(self):
        readable = self._available
        data = bytearray(readable)        
        if self._read_index + readable >= self._current_size:
            trimmed_length = self._current_size - self._read_index
            next_read_index = readable - trimmed_length
            array_copy(self._buffer, self._read_index, data,
                       0, trimmed_length)
            array_copy(self._buffer, 0, data,
                       trimmed_length, next_read_index)
        else:
            array_copy(self._buffer, self._read_index,
                       data, 0, readable)        
        return str(data)

    def clear(self):
        self._read_index = 0
        self._write_index = 0
        self._available = 0


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

    def empty(self):
        return self._position >= self._size

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
signal and, if present, a message payload. There is also the expectation that
the evente methods will return in a timely fashion, otherwise the handler risks
holding up the I/O polling loop.
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
    has disconnected. This disconnect may happen at any time or due to an
    error. The message argument of this method represents the address of the
    client that was connected.

    A handler may forward an event to the following handler by returning using
    the netpype.selector.FORWARD signal. The argument is passed to the next
    handler as its message.

    A handler may request socket events. Socket events break out of the
    pipeline and are acted upon immediately.

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

    A handler may request socket events. Socket events break out of the
    pipeline and are acted upon immediately.

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
