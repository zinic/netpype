import logging
#import cProfile

from multiprocessing import Process, Value


_LOG = logging.getLogger('netpype')

_STATE_NEW = 0
_STATE_RUNNING = 1
_STATE_STOPPED = 2

class PersistentProcess(object):

    def __init__(self, name, **kwargs):
        self._name = name
        self._state = Value('i', _STATE_NEW)
        self._process = Process(
            target=self._run, kwargs={'state': self._state})

    def stop(self):
        self._state.value = _STATE_STOPPED
        self._process.join()

    def start(self):
        if self._state.value != _STATE_NEW:
            raise WorkerStateError('Worker has been started once already.')

        self._state.value = _STATE_NEW
        self._process.start()

    def _run(self, state):
        self.on_start()

        while state.value == _STATE_NEW:
            try:
                self.process()
            except Exception as ex:
                _LOG.exception(ex)
                self._state.value = _STATE_STOPPED
        self.on_halt()

    def process(self):
        raise NotImplementedError

    def on_start(self):
        pass

    def on_halt(self):
        pass
