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

    def _handle_result(self, result):
        result_signal = result[0]
        result_fileno = result[1]

        channel_handler = self._active_channels[result_fileno]

        if channel_handler:
            _LOG.debug('Driving result {} for {}.'.format(
                result_signal, result_fileno))

            if result_signal == selection_events.REQUEST_READ:
                self._epoll.modify(
                    result_fileno, select.EPOLLIN)
            elif result_signal == selection_events.REQUEST_WRITE:
                channel_handler.write_buffer.set_buffer(result[2])
                self._epoll.modify(
                    result_fileno, select.EPOLLOUT)
            elif result_signal == selection_events.DISPATCH:
                self.dispatch((channel_handler.address, result[2]))
            elif result_signal == selection_events.REQUEST_CLOSE:
                channel_handler.write_buffer = None
                channel_handler.channel.shutdown(socket.SHUT_RDWR)
                self._network_event(
                    selection_events.CHANNEL_CLOSED,
                    result_fileno,
                    channel_handler.pipeline,
                    channel_handler.client_addr)
            elif result_signal == selection_events.RECLAIM_CHANNEL:
                self._epoll.unregister(result_fileno)
                channel = self._active_channels[result_fileno].channel
                del self._active_channels[result_fileno]
                channel.close()
            else:
                _LOG.debug('Unrecognized event: {} passed.'.format(
                    result_signal))
