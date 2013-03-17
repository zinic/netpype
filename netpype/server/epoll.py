import socket
import select
import netpype.env as env

from netpype import PersistentProcess
from netpype.server import SelectorServer
from netpype.channel import server_socket, HandlerPipeline, ChannelPipeline
from netpype.selector import events as selection_events


_LOG = env.get_logger('netpype.server.epoll')


class EPollSelectorServer(SelectorServer):

    def __init__(self, socket_addr, pipeline_factory):
        super(EPollSelectorServer, self).__init__(
            socket_addr, pipeline_factory)

    def on_start(self):
        super(EPollSelectorServer, self).on_start()
        self._epoll = select.epoll()
        self._epoll.register(self._socket_fileno, select.EPOLLIN)

    def on_halt(self):
        if hasattr(self, '_epoll'):
            self._epoll.unregister(self._socket_fileno)
            self._epoll.close()
        super(EPollSelectorServer, self).on_halt()

    def _poll(self):
        # Poll
        for fileno, event in self._epoll.poll(0.01):
            self._on_epoll(event, fileno)

    def _on_epoll(self, event, fileno):
        if fileno == self._socket_fileno:
            handler = self._accept(self._socket, self._pipeline_factory)
            self._epoll.register(handler.fileno)
            self._active_channels[handler.fileno] = handler
            self._network_event(
                selection_events.CHANNEL_CONNECTED,
                handler.fileno,
                handler.pipeline,
                handler.client_addr)
        else:
            channel_handler = self._active_channels[fileno]

            if event & select.EPOLLIN or event & select.EPOLLPRI:
                try:
                    read = channel_handler.channel.recv(1024)
                    if len(read) == 0:
                        raise IOError()
                    self._network_event(
                        selection_events.READ_AVAILABLE,
                        fileno,
                        channel_handler.pipeline,
                        read)
                except IOError:
                    self._network_event(
                        selection_events.CHANNEL_CLOSED,
                        channel_handler.fileno,
                        channel_handler.pipeline,
                        channel_handler.client_addr)
            elif event & select.EPOLLOUT:
                write_buffer = channel_handler.write_buffer
                if not write_buffer.empty():
                    try:
                        write_buffer.sent(channel_handler.channel.send(
                            write_buffer.remaining()))
                    except IOError:
                        self._network_event(
                            selection_events.CHANNEL_CLOSED,
                            channel_handler.fileno,
                            channel_handler.pipeline,
                            channel_handler.client_addr)
                    if write_buffer.empty():
                        self._network_event(
                            selection_events.WRITE_AVAILABLE,
                            fileno,
                            channel_handler.pipeline)
            elif event & select.EPOLLHUP:
                self._network_event(
                    selection_events.CHANNEL_CLOSED,
                    fileno,
                    channel_handler.pipeline,
                    channel_handler.client_addr)

    def _read_requested(self, fileno):
        self._epoll.modify(fileno, select.EPOLLIN)

    def _write_requested(self, fileno):
        self._epoll.modify(fileno, select.EPOLLOUT)

    def _channel_closed(self, fileno):
        self._epoll.unregister(fileno)
