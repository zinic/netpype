import socket
import select
import logging
import sys

from netpype import PersistentProcess
from netpype.channel import server_socket, HandlerPipeline, ChannelPipeline
from netpype.selector import events as selection_events


_LOG = logging.getLogger('netpype.server')


try:
    from billiard import Pool, cpu_count
except ImportError as ie:
    _LOG.warn('Billard library not available. Multiprocess will be used.')
    from multiprocessing import Pool, cpu_count


def network_event(signal, socket_fileno, handler_pipelines, data=None):
    if signal == selection_events.CHANNEL_CLOSED:
        one_way_dispatch('on_close', handler_pipelines.downstream, data)
        return (selection_events.RECLAIM_CHANNEL, socket_fileno, None)

    if signal == selection_events.CHANNEL_CONNECTED:
        function = 'on_connect'
        pipeline = handler_pipelines.downstream
    elif signal == selection_events.READ_AVAILABLE:
        function = 'on_read'
        pipeline = handler_pipelines.downstream
    elif signal == selection_events.WRITE_AVAILABLE:
        function = 'on_write'
        pipeline = handler_pipelines.upstream
    else:
        raise Exception('Unable to drive pipeline event: {}.'.format(signal))
    return pipeline_dispatch(function, socket_fileno, pipeline, data)


def one_way_dispatch(function, pipeline, data):
    msg_obj = data

    try:
        for handler in pipeline:
            getattr(handler, function)(msg_obj)
    except Exception as ex:
        _LOG.exception(ex)


def pipeline_dispatch(function, socket_fileno, pipeline, data):
    exit_signal = None
    msg_obj = data

    try:
        for handler in pipeline:
            result = getattr(handler, function)(msg_obj)\
            if result:
                exit_signal = result[0]
                msg_obj = result[1]
                if not exit_signal == selection_events.FORWARD:
                    break
    except Exception as ex:
        _LOG.exception(ex)

    if exit_signal:
        return (exit_signal, socket_fileno, msg_obj)
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
        self._workers = Pool(processes=cpu_count())
        self._socket = server_socket(self._socket_addr)
        self._socket_fileno = self._socket.fileno()

    def on_halt(self):
        self._socket.close()

    def dispatch(self, event):
        self._workers().apply()

    def _network_event(self, signal, fileno, pipeline, data=None):
        _LOG.debug('Driving event {} for {}.'.format(signal, fileno))
        result = network_event(signal, fileno, pipeline, data)
        if result:
            self._handle_result(result)

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
