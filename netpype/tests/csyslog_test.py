import unittest
import time

from  netpype.csyslog import SyslogMessageAccumulator, SyslogParser, SyslogLexer


HAPPY_PATH_MESSAGE = bytearray('263 <46>1 2012-12-11T15:48:23.217459-06:00 tohru ' +
                      'rsyslogd 6611 12512 [origin_1 software="rsyslogd" ' +
                      'swVersion="7.2.2" x-pid="12297" ' +
                      'x-info="http://www.rsyslog.com"]' +
                      '[origin_2 software="rsyslogd" swVersion="7.2.2" ' +
                      'x-pid="12297" x-info="http://www.rsyslog.com"] ' +
                      'start')


def chunk(data, limit, chunk_size=10):
    index = 0
    while index < limit:
        next_index = index + chunk_size
        end_index = next_index if next_index < limit else limit
        yield data[index:end_index]
        index = end_index


class MessageValidator(SyslogMessageAccumulator):
    pass


class WhenParsingSyslog(unittest.TestCase):

    def setUp(self):
        self.lexer = SyslogLexer()
        self.parser = SyslogParser(self.lexer)
        self.buffer = bytearray(2048)

    def test_read_octet_count(self):
        map(self.parser.read, chunk(HAPPY_PATH_MESSAGE, len(HAPPY_PATH_MESSAGE)))
        self.assertEqual(0, self.lexer.remaining())
        self.assertEqual('46', self.parser.message.priority)
        self.assertEqual('1', self.parser.message.version)
        self.assertEqual('2012-12-11T15:48:23.217459-06:00', self.parser.message.timestamp)
        self.assertEqual('tohru', self.parser.message.hostname)
        self.assertEqual('rsyslogd', self.parser.message.appname)
        self.assertEqual('6611', self.parser.message.processid)
        self.assertEqual('12512', self.parser.message.messageid)



def performance(duration=10, print_output=True):
    lexer = SyslogLexer()
    data_length = len(HAPPY_PATH_MESSAGE)
    runs = 0
    then = time.time()
    while time.time() - then < duration:
        map(self.parser.read, chunk(lexer, HAPPY_PATH_MESSAGE, 263))
        runs += 1
    if print_output:
        print('Ran {} times in {} seconds for {} runs per second.'.format(
            runs,
            duration,
            runs / float(duration)))


if __name__ == '__main__':
    print('Executing warmup')
    performance(10, False)
    print('Executing performance test')
    performance(5)

    print('Profiling...')
    import cProfile
    cProfile.run('performance(5)')
    #unittest.main()

if __name__ == '__main__':
    unittest.main()
