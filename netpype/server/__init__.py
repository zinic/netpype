import socket
import select
import logging
import sys

from netpype import PersistentProcess
from netpype.channel import server_socket, HandlerPipeline, ChannelPipeline
from netpype.selector import events as selection_events


_LOG = logging.getLogger('netpype.server')


def drive_event(signal, socket_fileno, handler_pipelines, data=None):
    assert_result = True

    if signal == selection_events.CHANNEL_CONNECTED:
        function = 'on_connect'
        pipeline = handler_pipelines.downstream
    elif signal == selection_events.READ_AVAILABLE:
        function = 'on_read'
        pipeline = handler_pipelines.downstream
    elif signal == selection_events.WRITE_AVAILABLE:
        function = 'on_write'
        pipeline = handler_pipelines.upstream
    elif signal == selection_events.CHANNEL_CLOSED:
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
                if not result_signal == selection_events.FORWARD:
                    exit_signal = result_signal
                    break
    except Exception as ex:
        _LOG.exception(ex)


    if signal == selection_events.CHANNEL_CLOSED:
        return (selection_events.RECLAIM_CHANNEL, socket_fileno, None)
    elif exit_signal:
        return (exit_signal, socket_fileno, msg_obj)
    else:
        return None


class SelectorServer(PersistentProcess):

    def __init__(self, socket_addr, pipeline_factory):
        super(SelectorServer, self).__init__(
            'SelectorServer - {}'.format(socket_addr))
        self._socket_addr = socket_addr
        self._pipeline_factory = pipeline_factory
        self._active_channels = dict()

    def on_start(self):
        # Init everything else we need now that we're in the sub-process
        # self._workers = Pool(processes=cpu_count())
        self._socket = server_socket(self._socket_addr)
        self._socket_fileno = self._socket.fileno()

    def on_halt(self):
        self._socket.close()

    def _drive_event(self, signal, fileno, pipeline, data=None):
        _LOG.debug('Driving event {} for {}.'.format(signal, fileno))
        self._handle_result(drive_event(signal, fileno, pipeline, data))

    def _accept(self, socket, pipeline_factory):
        # Gimme dat socket
        channel, address = socket.accept()
        fileno = channel.fileno()

        # Set non-blocking
        channel.setblocking(0)

        # Return a pipeline object
        return ChannelPipeline(
            channel,
            HandlerPipeline(pipeline_factory),
            address)

    def _handle_result(self, event):
        raise NotImplementedError

    def process(self):
        raise NotImplementedError
