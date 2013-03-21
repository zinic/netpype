import unittest
import time

import cProfile

from netpype.examples.syslog import SyslogLexer, lexer_states


HAPPY_PATH_MESSAGE = bytearray(b'263 <46>1 2012-12-11T15:48:23.217459-06:00 tohru ' +
                      b'rsyslogd 6611 12512 [origin_1 software="rsyslogd" ' +
                      b'swVersion="7.2.2" x-pid="12297" ' +
                      b'x-info="http://www.rsyslog.com"]' +
                      b'[origin_2 software="rsyslogd" swVersion="7.2.2" ' +
                      b'x-pid="12297" x-info="http://www.rsyslog.com"] ' +
                      b'start')


class WhenLexingSyslogHead(unittest.TestCase):

    def setUp(self):
        self.lexer = SyslogLexer()

    def test_connect(self):
        self.lexer.on_connect('localhost')
        self.assertEqual(lexer_states.START, self.lexer.get_state())

    def test_octet_count(self):
        self.lexer.on_connect('localhost')
        self.lexer.on_read(HAPPY_PATH_MESSAGE[:4])
        self.assertEqual(lexer_states.READ_PRI, self.lexer.get_state())
        self.assertEqual(259, self.lexer._octet_count)

    # Not sure if want
#    def test_octect_count_too_long(self):
#        self.lexer.on_connect('localhost')
#        self.assertRaises(Exception, self.lexer.on_read,
#                          b'5193859319513958131234')

    def test_pri(self):
        self.lexer.on_connect('localhost')        
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 8))
        self.assertEqual(lexer_states.READ_VERSION, self.lexer.get_state())
        self.assertEqual('46', self.lexer.get_message().priority)

    def test_version(self):
        self.lexer.on_connect('localhost')        
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 10))
        self.assertEqual(
            lexer_states.READ_TIMESTAMP, self.lexer.get_state())
        self.assertEqual('1', self.lexer.get_message().version)

    def test_timestamp(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 43))
        self.assertEqual(lexer_states.READ_HOSTNAME, self.lexer.get_state())
        self.assertEqual(
            '2012-12-11T15:48:23.217459-06:00',
            self.lexer.get_message().timestamp)

    def test_hostname(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 49))
        self.assertEqual(lexer_states.READ_APPNAME, self.lexer.get_state())
        self.assertEqual('tohru', self.lexer.get_message().hostname)

    def test_appname(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 58))
        self.assertEqual(lexer_states.READ_PROCESSID, self.lexer.get_state())
        self.assertEqual('rsyslogd', self.lexer.get_message().appname)

    def test_processid(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 64))
        self.assertEqual(lexer_states.READ_MESSAGEID, self.lexer.get_state())
        self.assertEqual('6611', self.lexer.get_message().processid)

    def test_messageid(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 69))
        self.assertEqual(
            lexer_states.READ_SD_ELEMENT, self.lexer.get_state())
        self.assertEqual('12512', self.lexer.get_message().messageid)

    def test_sd_element(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 70))
        self.assertEqual(
            lexer_states.READ_SD_ELEMENT_NAME, self.lexer.get_state())

    def test_sd_element_name(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 79))
        self.assertEqual(
            lexer_states.READ_SD_FIELD_NAME, self.lexer.get_state())

    def test_sd_field_name(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 88))
        self.assertEqual(
            lexer_states.READ_SD_VALUE_START, self.lexer.get_state())

    def test_sd_field_value_start(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 89))
        self.assertEqual(
            lexer_states.READ_SD_VALUE_CONTENT, self.lexer.get_state())

    def test_sd_field_value_content(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 98))
        self.assertEqual(
            lexer_states.READ_SD_NEXT_FIELD_OR_END, self.lexer.get_state())

    def test_sd(self):
        map(self.lexer.on_read,
            chunk(HAPPY_PATH_MESSAGE, len(HAPPY_PATH_MESSAGE)))
        def check(sd_element):
            self.assertEqual(
                'rsyslogd', str(sd_element.sd_field('software').value))
            self.assertEqual(
                '7.2.2', str(sd_element.sd_field('swVersion').value))
            self.assertEqual(
                '12297', str(sd_element.sd_field('x-pid').value))
            self.assertEqual(
                'http://www.rsyslog.com', 
                str(sd_element.sd_field('x-info').value))
        check(self.lexer._message.sd_element('origin_1'))
        check(self.lexer._message.sd_element('origin_2'))

    def test_msg(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read,
            chunk(HAPPY_PATH_MESSAGE, len(HAPPY_PATH_MESSAGE)))
        self.assertEqual(
            lexer_states.START, self.lexer.get_state())
        
def performance(duration=10, print_output=True):
    lexer = SyslogLexer()
    data_length = len(HAPPY_PATH_MESSAGE)
    runs = 0
    then = time.time()
    while time.time() - then < duration:
        map(lexer.on_read,
            chunk(HAPPY_PATH_MESSAGE, data_length, 1024))
        runs += 1
    if print_output:
        print('Ran {} times in {} seconds for {} runs per second.'.format(
            runs,
            duration,
            runs / float(duration)))


def chunk(data, limit, chunk_size=10):
    index = 0
    while index < limit:
        next_index = index + chunk_size
        end_index = next_index if next_index < limit else limit
        yield data[index:end_index]
        index = end_index


if __name__ == '__main__':
    cProfile.run('performance(10)')
    #print('Executing warm-up run')
    #performance(10, False)
    #print('Executing performance test')
    #performance(5)
    #unittest.main()
