import select
import netpype.env as env

from netpype.server import SelectorServer
from netpype.selector import events as selection_events


_LOG = env.get_logger('netpype.server.poll')


class PollSelectorServer(SelectorServer):

    def __init__(self, socket_addr, pipeline_factory):
        super(PollSelectorServer, self).__init__(
            socket_addr, pipeline_factory)

    def on_start(self):
        super(PollSelectorServer, self).on_start()
        self._select_poll = select.poll()
        self._select_poll.register(self._socket_fileno, select.POLLIN)

    def on_halt(self):
        if hasattr(self, '_select_poll'):
            self._select_poll.unregister(self._socket_fileno)
        super(PollSelectorServer, self).on_halt()

    def _poll(self):
        # Poll
        for fileno, event in self._select_poll.poll():
            self._on_poll(event, fileno)

    def _on_poll(self, event, fileno):
        if fileno == self._socket_fileno:
            handler = self._accept(self._socket, self._pipeline_factory)
            self._select_poll.register(handler.fileno)
            self._active_channels[handler.fileno] = handler
            self._network_event(
                selection_events.CHANNEL_CONNECTED,
                handler.fileno,
                handler.pipeline,
                handler.client_addr)
        else:
            channel_handler = self._active_channels[fileno]

            if event & select.POLLIN or event & select.POLLPRI:
                try:
                    read = channel_handler.channel.recv(1024)
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
            elif event & select.POLLOUT:
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
            elif event & select.POLLHUP:
                self._network_event(
                    selection_events.CHANNEL_CLOSED,
                    fileno,
                    channel_handler.pipeline,
                    channel_handler.client_addr)

    def _read_requested(self, fileno):
        self._select_poll.modify(fileno, select.POLLIN)

    def _write_requested(self, fileno):
        self._select_poll.modify(fileno, select.POLLOUT)

    def _channel_closed(self, fileno):
        self._select_poll.unregister(fileno)
