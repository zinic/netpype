import socket
import select
import errno
import netpype.env as env

from netpype import PersistentProcess
from netpype.channel import server_socket, HandlerPipeline, ChannelPipeline
from netpype.selector import events as selection_events
from multiprocessing import Pool, cpu_count


_LOG = env.get_logger('netpype.server')
_EMPTY_BUFFER = b''


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
            result = getattr(handler, function)(msg_obj)
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
        if hasattr(self, '_socket'):
            self._socket.close()
        if hasattr(self, '_workers'):
            self._workers.close()
            self._workers.join()

    def dispatch(self, event):
        self._workers().apply()

    def _network_event(self, signal, fileno, pipeline, data=None):
        try:
            result = network_event(signal, fileno, pipeline, data)
            if result:
                self._handle_result(result)
        except IOError as ioe:
            self._handle_result

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

    def _handle_result(self, result):
        result_signal = result[0]
        result_fileno = result[1]

        channel_handler = self._active_channels.get(result_fileno)
        if result_signal == selection_events.REQUEST_READ:
            self._read_requested(result_fileno)
        elif result_signal == selection_events.REQUEST_WRITE:
            channel_handler.write_buffer.set_buffer(result[2])
            self._write_requested(result_fileno)
        elif result_signal == selection_events.DISPATCH:
            self.dispatch((channel_handler.address, result[2]))
        elif result_signal == selection_events.REQUEST_CLOSE:
            channel_handler.write_buffer.set_buffer(_EMPTY_BUFFER)

            # Try to gracefully start the closing process for the socket
            try:
                channel_handler.channel.shutdown(socket.SHUT_RDWR)
            except IOError:
                pass

            self._channel_closed(result_fileno)
            self._network_event(
                selection_events.CHANNEL_CLOSED,
                channel_handler.fileno,
                channel_handler.pipeline,
                channel_handler.client_addr)
        elif result_signal == selection_events.RECLAIM_CHANNEL:
            del self._active_channels[result_fileno]
            try:
                channel_handler.channel.close()
            except IOError:
                pass
        else:
            _LOG.debug('Unrecognized event: {} passed.'.format(result_signal))

    def process(self):
        try:
            self._poll()
        except IOError as ioe:
            if ioe.errno == errno.EINTR:
                _LOG.warn('Interrupt caught, exiting.')
        except Exception as ex:
            _LOG.exception(ex)

    def _poll(self):
        raise NotImplementedError

    def _read_requested(self, fileno):
        raise NotImplementedError

    def _write_requested(self, fileno, data):
        raise NotImplementedError

    def _channel_closed(self, fileno):
        raise NotImplementedError
