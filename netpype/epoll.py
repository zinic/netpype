import socket
import select
import logging
import pickle
import ctypes
import time

from collections import deque
from multiprocessing import reduction, Pool, Queue, Value, cpu_count
from multiprocessing.forking import close as f_close
from netpype import PersistentProcess


_LOG = logging.getLogger('netpype.epoll')

_EPOLL_EVENTS = {
    select.EPOLLIN: 'Available for read',
    select.EPOLLOUT: 'Available for write',
    select.EPOLLPRI: 'Urgent data for read',
    select.EPOLLERR: 'Error condition happened on the associated fd',
    select.EPOLLHUP: 'Hang up happened on the associated fd',
    select.EPOLLET: 'Set Edge Trigger behavior, the default is Level Trigger behavior',
    select.EPOLLONESHOT: 'Set one-shot behavior. After one event is pulled out, the fd is internally disabled',
    select.EPOLLRDNORM: 'Equivalent to EPOLLIN',
    select.EPOLLRDBAND: 'Priority data band can be read.',
    select.EPOLLWRNORM: 'Equivalent to EPOLLOUT',
    select.EPOLLWRBAND: 'Priority data may be written.',
    select.EPOLLMSG: 'Ignored.'
}


UNIX_SOCK = socket.AF_UNIX
IPv4_SOCK = socket.AF_INET
IPv6_SOCK = socket.AF_INET6

def _new_server_socket(socket_type, bind_address, bind_port):
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


WRITE_AVAILABLE = 0
READ_AVAILABLE = 1
CONNECTED = 2
CLOSED = 3

WRITE_REQUEST = 100
READ_REQUEST = 101
CLOSE_REQUEST = 102

RELEASE_SOCKET = 200
RECLAIM_SOCKET = 201

class PipelineEvent(object):

    def __init__(self, signal):
        self.signal = signal


class PipelineMessage(PipelineEvent):

    def __init__(self, signal, payload=None):
        super(PipelineMessage, self).__init__(signal)
        self.payload = payload


class ChannelInterestEvent(PipelineEvent):

    def __init__(self, signal, socket_fileno):
        super(ChannelInterestEvent, self).__init__(signal)
        self.socket_fileno = socket_fileno


class ChannelConnectedEvent(PipelineEvent):

    def  __init__(self, address):
        super(ChannelConnectedEvent, self).__init__(CONNECTED)
        self.address = address


class ChannelClosedEvent(PipelineEvent):

    def  __init__(self, address):
        super(ChannelClosedEvent, self).__init__(CLOSED)
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


class ChannelManager(object):

    def __init__(self, socket_fileno, queue):
        self._queue = queue
        self._socket_fileno = socket_fileno

    def request_read(self):
        self._queue.put(ChannelInterestEvent(
            READ_REQUEST, self._socket_fileno))

    def request_write(self):
        self._queue.put(ChannelInterestEvent(
            WRITE_REQUEST, self._socket_fileno))

    def request_close(self):
        self._queue.put(ChannelInterestEvent(
            CLOSE_REQUEST, self._socket_fileno))


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


def drive_event(event, fileno, pipeline, handle=None):
    _LOG.debug('Driving event: {} for {}.'.format(event.signal, fileno))

    try:
        msg_obj = event
        channel_event = RELEASE_SOCKET

        if event.signal == CONNECTED:
            manager = ChannelManager(fileno, drive_event._queue)
            for handler in pipeline.downstream:
                next_msg = handler.on_connect(msg_obj, manager)
                msg_obj = next_msg if next_msg else msg_obj
        elif event.signal == READ_AVAILABLE:
            manager = ChannelManager(fileno, drive_event._queue)
            for handler in pipeline.downstream:
                next_msg = handler.on_read(msg_obj, manager)
                msg_obj = next_msg if next_msg else msg_obj
        elif event.signal == WRITE_AVAILABLE:
            manager = ChannelManager(fileno, drive_event._queue)
            for handler in pipeline.upstream:
                next_msg = handler.on_write(msg_obj, manager)
                msg_obj = next_msg if next_msg else msg_obj
        elif event.signal == CLOSED:
            channel_event = RECLAIM_SOCKET
            for handler in pipeline.downstream:
                next_msg = handler.on_close(msg_obj)
                msg_obj = next_msg if next_msg else msg_obj
        else:
            _LOG.error('Unable to drive pipeline event: {}.'.format(event.signal))
    except Exception as ex:
        _LOG.exception(ex)
    finally:
        if handle:
            handle.close()
        drive_event._queue.put(ChannelInterestEvent(channel_event, fileno))


def driver_init(queue):
    drive_event._queue = queue


class EPollServer(PersistentProcess):

    _active_channels = dict()

    def __init__(self, socket_info, pipeline_factory):
        super(EPollServer, self).__init__(
            'EPollServer - {}'.format(socket_info))
        try:
            self._epoll = select.epoll()
        except IOError as ioe:
            print(ioe)
        self._socket_info = socket_info
        self._pipeline_factory = pipeline_factory

    def publish(self, event):
        self._event_queue.put

    def on_start(self):
        self._event_queue = Queue()
        self._proc_pool = Pool(processes=cpu_count(), initializer=driver_init, initargs=[self._event_queue], maxtasksperchild=10240)
        self._socket = _new_server_socket(
            self._socket_info.socket_type,
            self._socket_info.bind_address,
            self._socket_info.bind_port)
        self._socket_fileno = self._socket.fileno()
        self._epoll.register(self._socket_fileno, select.EPOLLIN)

    def on_halt(self):
        self._epoll.unregister(self._socket_fileno)
        self._epoll.close()
        self._socket.close()
        self._proc_pool.close()

    def on_event(self, event):
        _LOG.error('Internal Event: {} targeting: {}.'.format(event.signal, event.socket_fileno))
        if event.signal == READ_REQUEST:
            self._epoll.modify(event.socket_fileno, select.EPOLLIN)
        elif event.signal == WRITE_REQUEST:
            self._epoll.modify(event.socket_fileno, select.EPOLLOUT)
        elif event.signal == CLOSE_REQUEST:
            self._epoll.modify(event.socket_fileno, select.EPOLLHUP)
            channel_info = self._active_channels[event.socket_fileno]
            self.dispatch(
                        ChannelClosedEvent(channel_info.address),
                        channel_info.fileno,
                        channel_info.pipeline)
        elif event.signal == RELEASE_SOCKET:
            _LOG.error('RELEASE {}'.format(event.socket_fileno))
            active_channel = self._active_channels.get(event.socket_fileno)
            if active_channel:
                active_channel.state.release()
        elif event.signal == RECLAIM_SOCKET:
            _LOG.error('RECLAIM {}'.format(event.socket_fileno))
            self._epoll.unregister(event.socket_fileno)
            channel = self._active_channels[event.socket_fileno].channel
            channel.shutdown(socket.SHUT_RDWR)
            channel.close()
            del self._active_channels[event.socket_fileno]
        else:
            _LOG.error('Unknown event signal: {} passed.'.format(event.signal))

    def on_epoll(self, event, fileno):
        if fileno == self._socket_fileno:
            channel_info = self._accept()
            self.dispatch(
                ChannelConnectedEvent(channel_info.address),
                channel_info.fileno,
                channel_info.pipeline)
        else:
            channel_info = self._active_channels[fileno]

            if not channel_info.state.in_use():# and channel_info.state.is_interested(event):
                channel_info.state.use(event)

                if event & select.EPOLLIN:
                    handle = channel_info.new_handle()
                    self.dispatch(
                        ChannelReadEvent(handle),
                        channel_info.fileno,
                        channel_info.pipeline,
                        handle)
                elif event & select.EPOLLOUT:
                    handle = channel_info.new_handle()
                    self.dispatch(
                        ChannelWriteEvent(handle),
                        channel_info.fileno,
                        channel_info.pipeline,
                        handle)
                elif event & select.EPOLLHUP:
                    self.dispatch(
                        ChannelClosedEvent(channel_info.address),
                        channel_info.fileno,
                        channel_info.pipeline)

    def dispatch(self, event, fileno, pipeline, handle=None):
        _LOG.debug('Dispatching {} to {} with handle {}.'.format(event, fileno, handle))
        self._proc_pool.apply_async(func=drive_event, args=(
                event,
                fileno,
                pipeline,
                handle))

    MAX_EVENTS = cpu_count()*2
    skip_epoll = False

    def process(self, kwargs):
        try:
            # Events take priority
            while not self._event_queue.empty():
                self.on_event(self._event_queue.get_nowait())

            # Poll
            if not self.skip_epoll:
                events = self._epoll.poll(timeout=0, maxevents=self.MAX_EVENTS)
                if len(events) > 0:
                    for fileno, event in events:
                        self.on_epoll(event, fileno)
                else:
                    self.skip_epoll = True
            else:
                self.skip_epoll = False
        except Exception as ex:
            _LOG.exception(ex)

    def _accept(self):
        channel, address = self._socket.accept()
        channel_info = ChannelDescriptor(channel, address, ChannelPipeline(self._pipeline_factory))
        _LOG.info('Connection accepted - fileno: {}'.format(channel_info.fileno))

        # Set non-blocking
        channel.setblocking(0)

        # Register with selector and set us ready to recieve
        self._epoll.register(channel_info.fileno , select.EPOLLIN)

        # Log this connection
        self._active_channels[channel_info.fileno] = channel_info
        return channel_info


class AbstractEPollHandler(object):

    def on_connect(self, event, manager):
        pass

    def on_close(self, event):
        pass

    def on_read(self, event, manager):
        return PipelineEvent(PASS)

    def on_write(self, event, manager):
        return PipelineEvent(PASS)
