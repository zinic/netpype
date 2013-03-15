import socket
import select
import logging

from netpype import PersistentProcess
from netpype.server import SelectorServer
from netpype.channel import server_socket, HandlerPipeline, ChannelPipeline
from netpype.selector import events as selection_events


_LOG = logging.getLogger('netpype.server.epoll')


class EPollSelectorServer(SelectorServer):

    def __init__(self, socket_addr, pipeline_factory):
        super(EPollSelectorServer, self).__init__(
            socket_addr, pipeline_factory)

    def on_start(self):
        super(EPollSelectorServer, self).on_start()
        self._epoll = select.epoll()
        self._epoll.register(self._socket_fileno, select.EPOLLIN)

    def on_halt(self):
        self._epoll.unregister(self._socket_fileno)
        self._epoll.close()
        super(EPollSelectorServer, self).on_halt()

    def process(self):
        try:
            # Poll
            for fileno, event in self._epoll.poll():
                self._on_epoll(event, fileno)
        except Exception as ex:
            _LOG.exception(ex)

    def _on_epoll(self, event, fileno):
        _LOG.debug('EPoll event {} targeting {}.'.format(event, fileno))

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
            channel_info = self._active_channels[fileno]

            if event & select.EPOLLIN or event & select.EPOLLPRI:
                read = channel_info.channel.recv(1024)
                self._network_event(
                    selection_events.READ_AVAILABLE,
                    fileno,
                    channel_info.pipeline,
                    read)
            elif event & select.EPOLLOUT:
                write_buffer = channel_info.write_buffer
                if write_buffer:
                    if write_buffer.has_data():
                        write_buffer.sent(channel_info.channel.send(
                            write_buffer.remaining()))
                        if not write_buffer.has_data():
                            self._network_event(
                                selection_events.WRITE_AVAILABLE,
                                fileno,
                                channel_info.pipeline)
                    else:
                        self._network_event(
                            selection_events.WRITE_AVAILABLE,
                            fileno,
                            channel_info.pipeline)
            elif event & select.EPOLLHUP:
                self._network_event(
                    selection_events.CHANNEL_CLOSED,
                    fileno,
                    channel_info.pipeline,
                    channel_info.client_addr)

    def _read_requested(self, fileno):
        self._epoll.modify(fileno, select.EPOLLIN)

    def _write_requested(self, fileno):
        self._epoll.modify(fileno, select.EPOLLOUT)

    def _channel_closed(self, channel_handler):
        channel_handler.write_buffer = None
        channel_handler.channel.shutdown(socket.SHUT_RDWR)
        self._epoll.unregister(channel_handler.fileno)
        self._network_event(
            selection_events.CHANNEL_CLOSED,
            channel_handler.fileno,
            channel_handler.pipeline,
            channel_handler.client_addr)
