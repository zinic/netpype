import socket
import select
import netpype.env as env


_LOG = env.get_logger('netpype.channel')
_EMPTY_BUFFER = b''

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

    def seek(self, delim, limit=-1):
        if self._has_elements:
            available = self.available()
            next_byte = bytearray(1)
            peek_offset = 0

            while peek_offset < available:
                if limit > 0:
                    limit -= 1
                elif limit == 0:
                    # TODO: Raise a more reasonable exception
                    raise Exception
                self._peek(next_byte, peek_offset)
                if next_byte[0] == delim:
                    return peek_offset
                peek_offset += 1
        return -1

    def skip_until(self, delim, limit=-1):
        seek_offset = self.seek(delim, limit)
        if seek_offset != -1:
            return self.skip(seek_offset)
        return -1 

    def get_until(self, delim, data, offset=0, limit=-1):
        seek_offset = self.seek(delim, limit)
        if seek_offset != -1:
            return self.get(data, offset, seek_offset)
        return -1

    def _peek(self, data, offset=0):
        if self._has_elements and self.available() > offset:
            read_index = self._read_index + offset
            if read_index >= self._current_size:
                read_index -= self._current_size
            array_copy(self._buffer, read_index, data, 0, 1)
            return 1
        return 0

    def get(self, data, offset=0, length=None):
        if length is None or length > self.available():
            readable = self.available()
        else:
            readable = length

        if self._has_elements:
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
            if self._read_index == self._write_index:
                self._has_elements = False
        return readable

    def put(self, data, offset=0, length=None):
        if not length:
            length = len(data)
        remaining = self.remaining()
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
        self._has_elements = True
        return length

    def grow(self, min_length):
        new_size = self._current_size * 2 * (
            int(min_length / self._current_size) + 1)
        new_buffer = bytearray(new_size)
        read = self.get(new_buffer, 0, new_size)
        self._buffer = new_buffer
        self._current_size = new_size
        self._read_index = 0
        self._write_index = read
        self._has_elements = True

    def remaining(self):
        if self._write_index == self._read_index and self._has_elements:
            return 0
        elif self._write_index < self._read_index:
            return self._read_index - self._write_index
        return self._current_size - self._write_index + self._read_index

    def available(self):
        if self._write_index == self._read_index and self._has_elements:
            return self._current_size
        elif self._write_index < self._read_index:
            return self._write_index + self._current_size - self._read_index
        return self._write_index - self._read_index

    def clear(self):
        self._has_elements = False
        self._read_index = 0
        self._write_index = 0

    def skip(self, length):
        bytes_skipped = length
        bytes_available = self.available()

        if length > bytes_available:
            bytes_skipped = bytes_available
            self._read_index = 0
            self._write_index = 0
        else:
            if self._read_index + length < self._current_size:
                self._read_index += length
            else:
                self._read_index = length - self._current_size
                self._read_index -= self._read_index

        if self._read_index == self._write_index:
            self._has_elements = False

        return bytes_skipped


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
