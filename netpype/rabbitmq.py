from kombu import Connection


class RabbitMQWorker(object):

    def __init__(self, name, connection_str):
        super(RabbitMQWorker, self).__init__(name, 10)
        self._connection_str = connection_str
        self.connection = None

#    def on_halt(self):
#        if self.connection and self.connection.connected:
#            self.connection.close()

    def run(self, **kwargs):
#        if self.connection is None:
#            self.connection = Connection(self._connection_str)
            #self.connection.connect()

        print('Would push {} onto the queue.'.format(kwargs['work_obj']))
