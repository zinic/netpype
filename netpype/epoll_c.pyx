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
from netpype.channel import *


_LOG = logging.getLogger('netpype.epoll')


class AbstractEPollHandler(object):

    def on_connect(self, event):
        return PipelineMessage(REQUEST_CLOSE)

    def on_close(self, event):
        pass

    def on_read(self, event):
        return PipelineMessage(REQUEST_CLOSE)

    def on_write(self, event):
        return PipelineMessage(REQUEST_CLOSE)


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
        elif event.signal == READ_AVAILABLE:
            function = 'on_read'
            pipeline = handler_pipeline.downstream
        elif event.signal == WRITE_AVAILABLE:
            function = 'on_write'
            pipeline = handler_pipeline.upstream
        elif event.signal == CHANNEL_CLOSED:
            function = 'on_close'
            pipeline = handler_pipeline.downstream
        else:
            _LOG.error('Unable to drive pipeline event: {}.'.format(event.signal))

        if handle:
            msg_obj = (signal, handle)
        else:
            # Custom arg for things like address
            msg_obj = (signal, event[3])

        for handler in pipeline:
            call = getattr(handler, function)
            result_signal, event_payload = call(msg_obj)

            if result_signal:
                if result_signal == FORWARD:
                    msg_obj = event_payload
                else:
                    exit_signal = handler_result
                    break
    except Exception as ex:
        _LOG.exception(ex)
    finally:
        if handle:
            handle.close()
        drive_event._queue.put((INTEREST_REQUEST, fileno, exit_signal))


def drive_event(event, fileno, handler_pipeline, handle=None):
    _LOG.debug('Driving event: {} for {}.'.format(event.signal, fileno))

    try:
        exit_signal = RECLAIM_CHANNEL

        if event.signal == CHANNEL_CONNECTED:
            function = 'on_connect'
            pipeline = handler_pipeline.downstream
        elif event.signal == READ_AVAILABLE:
            function = 'on_read'
            pipeline = handler_pipeline.downstream
        elif event.signal == WRITE_AVAILABLE:
            function = 'on_write'
            pipeline = handler_pipeline.upstream
        elif event.signal == CHANNEL_CLOSED:
            function = 'on_close'
            pipeline = handler_pipeline.downstream
        else:
            _LOG.error('Unable to drive pipeline event: {}.'.format(event.signal))

        msg_obj = event

        for handler in pipeline:
            call = getattr(handler, function)
            handler_result = call(msg_obj)

            if handler_result:
                if handler_result.signal == FORWARD:
                    msg_obj = handler_result.payload
                else:
                    exit_signal = handler_result.signal
                    break
    except Exception as ex:
        _LOG.exception(ex)
    finally:
        if handle:
            handle.close()
        drive_event._queue.put(ChannelInterestEvent(INTEREST_REQUEST, exit_signal, fileno))


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
        self._proc_pool = Pool(processes=2, initializer=driver_init, initargs=[self._event_queue], maxtasksperchild=10240)
        self._socket = new_serversocket(
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
        _LOG.debug('Internal event: {}'.format(event))
        signal = event[0]

        if signal == INTEREST_REQUEST:
            interest = event[1]
            socket_fileno = event[2]

            _LOG.debug('Interest for {} changed to: {}.'.format(socket_fileno, interest))

            if interest == REQUEST_READ:
                self._epoll.modify(socket_fileno, select.EPOLLIN | select.EPOLLONESHOT)
            elif interest == REQUEST_WRITE:
                self._epoll.modify(socket_fileno, select.EPOLLOUT | select.EPOLLONESHOT)
            elif interest == REQUEST_CLOSE:
                channel_info = self._active_channels[socket_fileno]
                self.dispatch(
                    ChannelClosedEvent(channel_info.address),
                    channel_info.fileno,
                    channel_info.pipeline)
            elif interest == RECLAIM_CHANNEL:
                self._epoll.unregister(socket_fileno)
                channel = self._active_channels[socket_fileno].channel
                channel.shutdown(socket.SHUT_RDWR)
                channel.close()
                del self._active_channels[socket_fileno]

    def on_epoll(self, event, fileno):
        _LOG.debug('EPoll event {} targeting {}.'.format(event, fileno))

        if fileno == self._socket_fileno:
            channel_info = self._accept()
            self.dispatch(
                (CHANNEL_CONNECTED,
                    channel_info.fileno,
                    channel_info.pipeline,
                    channel_info.address))
        else:
            channel_info = self._active_channels[fileno]

            if event & select.EPOLLIN:
                handle = channel_info.new_handle()
                self.dispatch(
                    (READ_AVAILABLE,
                        channel_info.fileno,
                        channel_info.pipeline),
                    handle)
            elif event & select.EPOLLOUT:
                handle = channel_info.new_handle()
                self.dispatch(
                    (WRITE_AVAILABLE,
                        channel_info.fileno,
                        channel_info.pipeline),
                    handle)
            elif event & select.EPOLLHUP:
                self.dispatch(
                    (CHANNEL_CLOSED,
                        channel_info.fileno,
                        channel_info.pipeline,
                        channel_info.address))

    def dispatch(self, event, handle=None):
        self._proc_pool.apply_async(
            func=drive_event,
            args=(event, handle))

    def process(self, kwargs):
        try:
            # Events take priority
            while not self._event_queue.empty():
                _LOG.error('ass')
                self.on_event(self._event_queue.get_nowait())

            # Poll
            for fileno, event in self._epoll.poll(timeout=0.001):
                self.on_epoll(event, fileno)
        except Exception as ex:
            _LOG.exception(ex)

    def _accept(self):
        channel, address = self._socket.accept()
        channel_info = ChannelDescriptor(channel, address, ChannelPipeline(self._pipeline_factory))
        _LOG.info('Connection accepted - fileno: {}'.format(channel_info.fileno))

        # Set non-blocking
        channel.setblocking(0)
        self._epoll.register(channel_info.fileno, select.EPOLLONESHOT)

        # Log this connection
        self._active_channels[channel_info.fileno] = channel_info
        return channel_info
