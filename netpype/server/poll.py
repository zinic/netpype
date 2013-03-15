import socket
import select
import logging

#from billiard import Pool, cpu_count
from netpype import PersistentProcess
from netpype.server import SelectorServer
from netpype.channel import server_socket, HandlerPipeline, ChannelPipeline
from netpype.selector import events as selection_events


_LOG = logging.getLogger('netpype.server.poll')


class PollSelectorServer(SelectorServer):

    def __init__(self, socket_addr, pipeline_factory):
        super(PollSelectorServer, self).__init__(
            socket_addr, pipeline_factory)

    def on_start(self):
        super(PollSelectorServer, self).on_start()
        self._poll = select.poll()
        self._poll.register(self._socket_fileno, select.POLLIN)

    def on_halt(self):
        self._poll.unregister(self._socket_fileno)
        self._poll.close()
        super(PollSelectorServer, self).on_halt()

    def process(self):
        try:
            # Poll
            for fileno, event in self._poll.poll():
                self._on_poll(event, fileno)
        except Exception as ex:
            _LOG.exception(ex)

    def _on_poll(self, event, fileno):
        _LOG.debug('Poll event {} targeting {}.'.format(event, fileno))

        if fileno == self._socket_fileno:
            handler = self._accept(self._socket, self._pipeline_factory)
            self._poll.register(handler.fileno)
            self._active_channels[handler.fileno] = handler
            self._network_event(
                selection_events.CHANNEL_CONNECTED,
                handler.fileno,
                handler.pipeline,
                handler.client_addr)
        else:
            channel_info = self._active_channels[fileno]

            if event & select.POLLIN or event & select.POLLPRI:
                read = channel_info.channel.recv(1024)
                self._network_event(
                    selection_events.READ_AVAILABLE,
                    fileno,
                    channel_info.pipeline,
                    read)
            elif event & select.POLLOUT:
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
            elif event & select.POLLHUP:
                self._network_event(
                    selection_events.CHANNEL_CLOSED,
                    fileno,
                    channel_info.pipeline,
                    channel_info.client_addr)

    def _read_requested(self, fileno):
        self._epoll.modify(fileno, select.POLLIN)

    def _write_requested(self, fileno):
        self._epoll.modify(fileno, select.POLLOUT)

    def _channel_closed(self, channel_handler):
        channel_handler.write_buffer = None
        channel_handler.channel.shutdown(socket.SHUT_RDWR)
        self._epoll.unregister(channel_handler.fileno)
        self._network_event(
            selection_events.CHANNEL_CLOSED,
            channel_handler.fileno,
            channel_handler.pipeline,
            channel_handler.client_addr)
