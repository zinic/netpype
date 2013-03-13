import logging
import cProfile

from multiprocessing import Process, Value


_LOG = logging.getLogger('netpype')


class PersistentProcess(object):

    _STATE_NEW = 0
    _STATE_RUNNING = 1
    _STATE_STOPPED = 2

    def __init__(self, name, **kwargs):
        self._name = name
        self._state = Value('i', self._STATE_NEW)
        kwargs['state'] = self._state
        #self._process = Process(target=self._run_profiled, kwargs=kwargs)
        self._process = Process(target=self._run, kwargs=kwargs)

    def stop(self):
        _LOG.debug('Stopping process: {}'.format(self._name))
        self._state.value = self._STATE_STOPPED
        self._process.join()
        _LOG.debug('Process stopped: {}'.format(self._name))

    def start(self):
        _LOG.debug('Starting process: {}'.format(self._name))

        if self._state.value != self._STATE_NEW:
            raise WorkerStateError(
                'Worker has been started once already.')

        self._state.value = self._STATE_NEW
        self._process.start()
        _LOG.debug('Process {} started.'.format(self._name))

    def _run_profiled(self, state, **kwargs):
        cProfile.runctx('self._run(state, **kwargs)', globals(), locals())

    def _run(self, state, **kwargs):
        self.on_start()

        while state.value == self._STATE_NEW:
            try:
                self.process(kwargs)
            except Exception as ex:
                _LOG.exception(ex)
                self._state.value = self._STATE_STOPPED
        self.on_halt()

    def process(self, kwargs):
        pass

    def on_start(self):
        pass

    def on_halt(self):
        pass
