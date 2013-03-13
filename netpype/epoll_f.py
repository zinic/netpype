import socket
import select
import logging
import pickle
import ctypes
import time

from collections import deque
from multiprocessing import reduction, Pool, Pipe, Value, cpu_count
from multiprocessing.forking import close as f_close
from netpype import PersistentProcess
from netpype.channel import new_serversocket

_LOG = logging.getLogger('netpype.epoll')


UNIX_SOCK = socket.AF_UNIX
IPv4_SOCK = socket.AF_INET
IPv6_SOCK = socket.AF_INET6

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


class SocketINetAddress(object):

    def __init__(self, socket_type, bind_address, bind_port):
        self.socket_type = socket_type
        self.bind_address = bind_address
        self.bind_port = bind_port

    def __repr__(self):
        return '{} socket {}:{}'.format(self.socket_type,
                                        self.bind_address,
                                        self.bind_port)


class ChannelPipeline(object):

    def __init__(self, pipeline_factory):
        self.upstream = pipeline_factory.new_upstream_pipeline()
        self.downstream = pipeline_factory.new_upstream_pipeline()


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

    def __init__(self, handle, address, pipeline):
        self.handle = handle
        self.address = address
        self.pipeline = pipeline

    def fileno(self):
        return self.handle.fileno


class AbstractEPollHandler(object):

    def on_connect(self, address):
        return REQUEST_CLOSE, None

    def on_close(self, address):
        return None

    def on_read(self, channel):
        return REQUEST_CLOSE, None

    def on_write(self, channel):
        return REQUEST_CLOSE, None

_EPOLL = select.epoll()


def on_event(event, epoll, proc_pool, handle):
    signal = event[0]
    _LOG.info('Event {} caught from async call.'.format(signal))

    if signal == INTEREST_REQUEST:
        fileno = event[1]
        interest = event[2]

        if interest == REQUEST_READ:
            epoll.modify(fileno, select.EPOLLIN | select.EPOLLONESHOT)
        elif interest == REQUEST_WRITE:
            epoll.modify(fileno, select.EPOLLOUT | select.EPOLLONESHOT)
        elif interest == REQUEST_CLOSE:
            proc_pool.apply_async(
                func=drive_event,
                args=(CHANNEL_CLOSED,
                    channel_info.fileno(),
                    channel_info.pipeline,
                    channel_info.address))
        elif interest == RECLAIM_CHANNEL:
            channel = handle.channel
            channel.shutdown(socket.SHUT_RDWR)
            channel.close()
        _LOG.debug('Interest for {} changed to: {}.'.format(fileno, interest))
    elif signal == SOCKET_NOTICE:
        notice = event[1]

        if notice == CHANNEL_CONNECTED:
            channel_handle = event[2]
            address = event[3]

            _LOG.debug('Dispatching channel connect for fileno: {}.'.format(channel_handle.fileno))

            channel_info = ChannelDescriptor(
                channel_handle,
                address,
                ChannelPipeline(self._pipeline_factory))

            # We use oneshot for less noisy epoll event management
            epoll.register(channel_info.fileno(), select.EPOLLONESHOT)

            # Log this connection
            self._active_channels[channel_info.fileno()] = channel_info

            # Tell the pipeline
            self._dispatch((
                    CHANNEL_CONNECTED,
                    channel_info.fileno(),
                    channel_info.pipeline,
                    channel_info.address))
        elif notice == CHANNEL_CLOSED:
            fileno = event[2]
            _LOG.debug('Dispatching channel close for fileno: {}.'.format(fileno))
            channel_info = self._active_channels[fileno]
            self._dispatch((
                    CHANNEL_CLOSED,
                    channel_info.fileno(),
                    channel_info.pipeline,
                    channel_info.address))
        elif notice == READ_AVAILABLE or notice == WRITE_AVAILABLE:
            fileno = event[2]
            _LOG.debug('Dispatching {} for fileno: {}.'.format(notice, fileno))
            channel_info = self._active_channels[fileno]
            self._dispatch((
                    notice,
                    channel_info.fileno(),
                    channel_info.pipeline),
                channel_info.handle)


def drive_event(event, handle=None):
    signal = event[0]
    fileno = event[1]
    handler_pipeline = event[2]

    _LOG.debug('Driving event: {} for {}.'.format(signal, fileno))

    try:
        exit_signal = RECLAIM_CHANNEL

        if signal == CHANNEL_CONNECTED:
            function = 'on_connect'
            pipeline = handler_pipeline.downstream
        elif signal == READ_AVAILABLE:
            function = 'on_read'
            pipeline = handler_pipeline.downstream
        elif signal == WRITE_AVAILABLE:
            function = 'on_write'
            pipeline = handler_pipeline.upstream
        elif signal == CHANNEL_CLOSED:
            function = 'on_close'
            pipeline = handler_pipeline.downstream
        else:
            _LOG.error('Unable to drive pipeline event: {}.'.format(signal))

        if handle:
            msg_obj = handle.get_channel()
        else:
            # Custom arg for things like address
            msg_obj = event[3]

        for handler in pipeline:
            call = getattr(handler, function)
            result = call(msg_obj)

            if result:
                result_len = len(result)
                result_signal = result[0]
                if result_signal == FORWARD:
                    msg_obj = result[1] if result_len > 1 else None
                else:
                    exit_signal = result_signal
                    break
    except Exception as ex:
        _LOG.exception(ex)
    finally:
        if handle:
            handle.close()
        return (INTEREST_REQUEST, fileno, exit_signal)


class EPollServer(PersistentProcess):

    def __init__(self, socket_info, pipeline_factory):
        super(EPollServer, self).__init__(
            'EPollServer - {}'.format(socket_info))
        self._socket_info = socket_info
        self._pipeline_factory = pipeline_factory
        self._active_channels = dict()

    def on_start(self):
        self._socket = new_serversocket(
            self._socket_info.socket_type,
            self._socket_info.bind_address,
            self._socket_info.bind_port)
        self._socket_fileno = self._socket.fileno()
        self._epoll = _EPOLL
        self._epoll.register(self._socket_fileno, select.EPOLLIN)
        self._proc_pool = Pool(processes=cpu_count())

    def on_halt(self):
        self._epoll.unregister(self._socket_fileno)
        self._epoll.close()
        self._socket.close()
        self._proc_pool.close()

    def process(self, kwargs):
        try:
            # Poll
            for fileno, event in self._epoll.poll():
                self._on_epoll(event, fileno)
        except Exception as ex:
            _LOG.exception(ex)

    def _dispatch_interest(self, event, handle=None):
        self._proc_pool.apply_async(
            func=drive_event,
            args=(event, handle),
            callback=self._on_event)

    def _on_epoll(self, event, fileno):
        if fileno == self._socket_fileno:
            self._accept()
        else:
            channel_info = self._active_channels.get(fileno)

            if channel_info:
                _LOG.debug('EPoll event {} targeting {}.'.format(event, fileno))

                if event & select.EPOLLIN:
                    self._dispatch_interest(
                        (SOCKET_NOTICE, READ_AVAILABLE, fileno),
                        channel_info.handle)
                elif event & select.EPOLLOUT:
                    self._dispatch_interest(
                        (SOCKET_NOTICE, WRITE_AVAILABLE, fileno),
                        channel_info.handle)
                elif event & select.EPOLLHUP:
                    self._epoll.unregister(fileno)
                    del self._active_channels[fileno]

    def _accept(self):
        # Gimme dat socket
        channel, address = self._socket.accept()

        # Set non-blocking
        channel.setblocking(0)

        # Distill handle`
        channel_handle = ChannelHandle(channel.fileno(), channel.type)

        # Log this connection
        self._dispatch_interest((SOCKET_NOTICE, CHANNEL_CONNECTED, channel_handle, address))
