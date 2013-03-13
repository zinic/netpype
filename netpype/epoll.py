import socket
import select
import logging

from multiprocessing import Pool, cpu_count
from netpype import PersistentProcess
from netpype.channel import server_socket, ChannelHandler


_LOG = logging.getLogger('netpype.epoll')
_EPOLL = select.epoll()
_EMPTY_BUFFER = b''

# Event signals
SOCKET_NOTICE = 0
WRITE_AVAILABLE = 1
READ_AVAILABLE = 2
CHANNEL_CONNECTED = 3
CHANNEL_CLOSED = 4
INTEREST_REQUEST = 5
RECLAIM_CHANNEL = 6
REQUEST_WRITE = 100
REQUEST_READ = 101
REQUEST_CLOSE = 102
FORWARD = 103


def drive_event(signal, socket_fileno, handler_pipelines, data=None):
    assert_result = True

    if signal == CHANNEL_CONNECTED:
        function = 'on_connect'
        pipeline = handler_pipelines.downstream
    elif signal == READ_AVAILABLE:
        function = 'on_read'
        pipeline = handler_pipelines.downstream
    elif signal == WRITE_AVAILABLE:
        function = 'on_write'
        pipeline = handler_pipelines.upstream
    elif signal == CHANNEL_CLOSED:
        function = 'on_close'
        pipeline = handler_pipelines.downstream
        assert_result = False
    else:
        raise Exception('Unable to drive pipeline event: {}.'.format(signal))

    exit_signal = None
    if data:
        msg_obj = data
    else:
        msg_obj = None

    try:
        for handler in pipeline:
            result = getattr(handler, function)(msg_obj)

            if assert_result and result:
                result_signal = result[0]
                msg_obj = result[1]
                if not result_signal == FORWARD:
                    exit_signal = result_signal
                    break
    except Exception as ex:
        _LOG.exception(ex)

    if exit_signal:
        return (exit_signal, socket_fileno, msg_obj)
    else:
        return None


class EPollServer(PersistentProcess):

    def __init__(self, socket_addr, pipeline_factory):
        super(EPollServer, self).__init__(
            'EPollServer - {}'.format(socket_addr))
        self._socket_addr = socket_addr
        self._pipeline_factory = pipeline_factory
        self._active_channels = dict()

    def on_start(self):
        self._socket = server_socket(self._socket_addr)
        self._socket_fileno = self._socket.fileno()
        self._epoll = _EPOLL
        self._epoll.register(self._socket_fileno, select.EPOLLIN)

    def on_halt(self):
        self._epoll.unregister(self._socket_fileno)
        self._epoll.close()
        self._socket.close()

    def _on_epoll(self, event, fileno):
        _LOG.debug('EPoll event {} targeting {}.'.format(event, fileno))

        if fileno == self._socket_fileno:
            handler, address = self._accept()
            self._drive_event(
                CHANNEL_CONNECTED,
                handler.channel.fileno(),
                handler.pipeline,
                address)
        else:
            channel_info = self._active_channels[fileno]

            if event & select.EPOLLIN:
                read = channel_info.channel.recv(1024)
                self._drive_event(
                    READ_AVAILABLE,
                    fileno,
                    channel_info.pipeline,
                    read)
            elif event & select.EPOLLOUT:
                if channel_info.write_buffer:
                    buffer_size = len(channel_info.write_buffer)
                else:
                    buffer_size = 0

                if buffer_size > 0:
                    sent = channel_info.channel.send(channel_info.write_buffer)
                    if sent < buffer_size:
                        next_chunk = channel_info.write_buffer[sent:]
                        channel_info.write_buffer = next_chunk
                    else:
                        channel_info.write_buffer = b''
                        self._drive_event(
                            WRITE_AVAILABLE,
                            fileno,
                            channel_info.pipeline)
                else:
                    self._drive_event(
                        WRITE_AVAILABLE,
                        fileno,
                        channel_info.pipeline)
            elif event & select.EPOLLHUP:
                self._drive_event(
                    CHANNEL_CLOSED,
                    fileno,
                    channel_info.pipeline)

    def _drive_event(self, signal, fileno, pipeline, data=None):
        _LOG.debug('Driving event {} for {}.'.format(signal, fileno))
        result = drive_event(signal, fileno, pipeline, data)

        if result:
            result_signal = result[0]
            result_fileno = result[1]

            channel_handler = self._active_channels[result_fileno]

            if channel_handler:
                _LOG.debug('Driving result {} for {}.'.format(
                    result_signal, result_fileno))

                if result_signal == REQUEST_READ:
                    self._epoll.modify(
                        result_fileno, select.EPOLLIN | select.EPOLLONESHOT)
                elif result_signal == REQUEST_WRITE:
                    channel_handler.write_buffer = result[2]
                    self._epoll.modify(
                        result_fileno, select.EPOLLOUT | select.EPOLLONESHOT)
                elif result_signal == REQUEST_CLOSE:
                    self._epoll.unregister(result_fileno)
                    channel = self._active_channels[result_fileno].channel
                    del self._active_channels[result_fileno]
                    channel.shutdown(socket.SHUT_RDWR)
                    channel.close()

    def process(self, kwargs):
        try:
            # Poll
            for fileno, event in self._epoll.poll(0.1):
                self._on_epoll(event, fileno)
        except Exception as ex:
            _LOG.exception(ex)

    def _accept(self):
        # Gimme dat socket
        channel, address = self._socket.accept()
        fileno = channel.fileno()

        # Set non-blocking
        channel.setblocking(0)

        # Register
        self._epoll.register(fileno, select.EPOLLONESHOT)
        handler = ChannelHandler(
            channel, ChannelPipeline(self._pipeline_factory))
        self._active_channels[fileno] = handler
        return handler, address
