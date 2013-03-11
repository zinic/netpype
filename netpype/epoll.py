import socket
import select
import logging
import pickle

from collections import deque
from multiprocessing import reduction, Pool, Queue, cpu_count
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

WRITE_REQUEST = 100
READ_REQUEST = 101
CLOSE_REQUEST = 102

PASS = 200
BYTES_READ = 201


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


class ChannelEvent(PipelineEvent):

    def  __init__(self, signal, handle):
        super(ChannelEvent, self).__init__(signal)
        self._handle = handle


class ChannelReadEvent(ChannelEvent):

    def  __init__(self, handle):
        super(ChannelReadEvent, self).__init__(READ_AVAILABLE, handle)

    def read(self):
        return self._handle.get_channel().recv(1024)


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


class PipelineDriverError(Exception):

    def __init__(self, msg):
        self.msg = msg


class ChannelHandle(object):

    def __init__(self, fileno, socket_type, reduce_handle=True):
        self.fileno = fileno
        self.socket_type = socket_type
        self.has_handle = reduce_handle
        self._channel = None

        if reduce_handle:
            self._handle = reduction.reduce_handle(fileno)

    def get_channel(self):
        if not self.has_handle:
            raise Exception('No handle reduced...')

        if not self._channel:
            channel_fd = reduction.rebuild_handle(self._handle)
            self._channel = socket.fromfd(channel_fd, self.socket_type, socket.SOCK_STREAM)
            self._channel.setblocking(0)
        return self._channel

    def close(self):
        if self._channel:
            self._channel.close()


def drive_event(event, channel_handle, pipeline):
    _LOG.debug('Driving event: {}.'.format(event.signal))

    try:
        channel = channel_handle.get_channel()
        manager = ChannelManager(channel_handle.fileno, drive_event._queue)

        if event.signal == READ_AVAILABLE:
            _LOG.debug('Dispatching READ_AVAILABLE to {} downstream pipeline handlers'.format(len(pipeline.downstream)))
            msg_obj = event
            for handler in pipeline.downstream:
                msg_obj = handler.on_read(msg_obj, manager)
        elif event.signal == WRITE_AVAILABLE:
            _LOG.debug('Dispatching WRITE_AVAILABLE to {} upstream pipeline handlers'.format(len(pipeline.upstream)))
            msg_obj = u''
            for handler in pipeline.upstream:
                msg_obj = handler.on_write(msg_obj, manager)
            event.send(msg_obj)
        elif event.signal == CONNECTED:
            _LOG.debug('Dispatching CONNECTED to {} downstream pipeline handlers'.format(len(pipeline.downstream)))
            msg_obj = event
            for handler in pipeline.downstream:
                msg_obj = handler.on_connect(msg_obj, manager)
        else:
            _LOG.error('Unable to drive event: {}.'.format(event.signal))
    except Exception as ex:
        _LOG.exception(ex)
        return
    finally:
        channel_handle.close()


def driver_init(queue):
    drive_event._queue = queue


class EPollServer(PersistentProcess):

    _connections = dict()
    network_events = dict()
    epoll_queue = deque()

    def __init__(self, socket_info, pipeline_factory):
        super(EPollServer, self).__init__(
            'EPollServer - {}'.format(socket_info))
        try:
            self._epoll = select.epoll()
        except IOError as ioe:
            print(ioe)
        self.drain_at = 255000
        self.drain_to = self.drain_at / 2
        self._draining = False
        self._socket_info = socket_info
        self._pipeline_factory = pipeline_factory

    def publish(self, event):
        self._event_queue.put

    def on_start(self):
        self._event_queue = Queue()
        self._proc_pool = Pool(processes=2, initializer=driver_init, initargs=[self._event_queue])
        self._socket = _new_server_socket(
            self._socket_info.socket_type,
            self._socket_info.bind_address,
            self._socket_info.bind_port)
        self._epoll.register(self._socket.fileno(), select.EPOLLIN)

    def on_halt(self):
        self._epoll.unregister(self._socket.fileno())
        self._epoll.close()
        self._socket.close()
        self._proc_pool.close()

    def on_event(self, event):
        _LOG.debug('Internal Event: {}.'.format(event.signal))
        if event.signal == READ_REQUEST:
            self._epoll.modify(event.socket_fileno, select.EPOLLIN)
        elif event.signal == WRITE_REQUEST:
            self._epoll.modify(event.socket_fileno, select.EPOLLOUT)
        elif event.signal == CLOSE_REQUEST:
            self._epoll.unregister(event.socket_fileno)
            self._connections[event.socket_fileno]['connection'].close()
            del self._connections[event.socket_fileno]
        else:
            logger.error('Unknown event signal: {} passed.'.format(event.signal))

    def on_epoll(self, event, fileno):
        if fileno == self._socket.fileno():
            connection_info = self._accept()
            handle = ChannelHandle(fileno, connection_info['connection'].type, reduce_handle=False)
            self.dispatch(
                ChannelConnectedEvent(connection_info['address']),
                handle,
                connection_info['pipeline'])
        elif event & select.EPOLLIN:
            connection_info = self._connections[fileno]
            handle = ChannelHandle(fileno, connection_info['connection'].type)
            self.dispatch(
                ChannelReadEvent(handle),
                handle,
                connection_info['pipeline'])
#        elif event & select.EPOLLOUT:
#            connection_info = self._connections[fileno]
#            self.dispatch(ChannelWriteEvent(
#                connection_info['connection']),
#                connection_info)
#        elif event & select.EPOLLHUP:
#            pass, socket_type

    def dispatch(self, event, channel_handle, pipeline):
        _LOG.debug('Dispatching event: {}.'.format(event.signal))
        self._proc_pool.apply_async(drive_event, args=(
                event,
                channel_handle,
                pipeline))

    def process(self, kwargs):
        # Events can wait if we have a lot stacked up
        epoll_queue_len = len(self.epoll_queue)

        if not self._draining:
            if self.drain_at > epoll_queue_len:
                for fileno, event in self._epoll.poll(1):
                    self.epoll_queue.append((event, fileno))
            else:
                self._draining = True
        elif self.drain_to > epoll_queue_len:
            self._draining = False

        # Events take priority
        if not self._event_queue.empty():
            self.on_event(self._event_queue.get_nowait())

        # Process one epoll event
        if len(self.epoll_queue) > 0:
            event, fileno = self.epoll_queue.popleft()
            self.on_epoll(event, fileno )

    def _accept(self):
        connection, address = self._socket.accept()
        fileno = connection.fileno()
        _LOG.info('Connection accepted - fileno: {}'.format(fileno))

        # Set non-blocking
        connection.setblocking(0)

        # Register with selector and set us ready to recieve
        self._epoll.register(fileno , select.EPOLLIN)

        # Log this connection
        connection_info = {
            'connection': connection,
            'address' : address,
            'manager': ChannelManager(fileno, self),
            'pipeline' : ChannelPipeline(self._pipeline_factory)
        }
        self._connections[fileno] = connection_info
        return connection_info


class AbstractEPollHandler(object):

    def on_connect(self, event, manager):
        pass

    def on_close(self, event, manager):
        pass

    def on_read(self, event, manager):
        return PipelineEvent(PASS)

    def on_write(self, event, manager):
        return PipelineEvent(PASS)
