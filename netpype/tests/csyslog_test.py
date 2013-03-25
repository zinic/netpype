import unittest
import time

from  netpype.csyslog import SyslogLexer, read_into


HAPPY_PATH_MESSAGE = bytearray('263 <46>1 2012-12-11T15:48:23.217459-06:00 tohru ' +
                      'rsyslogd 6611 12512 [origin_1 software="rsyslogd" ' +
                      'swVersion="7.2.2" x-pid="12297" ' +
                      'x-info="http://www.rsyslog.com"]' +
                      '[origin_2 software="rsyslogd" swVersion="7.2.2" ' +
                      'x-pid="12297" x-info="http://www.rsyslog.com"] ' +
                      'start')


def chunk(lexer, data, limit, chunk_size=10):
    index = 0
    while index < limit:
        next_index = index + chunk_size
        end_index = next_index if next_index < limit else limit
        yield [data[index:end_index], end_index - index, lexer]
        index = end_index


def hand_to_lexer(args):
    lexer = args[2]
    read_into(args[0], 0, args[1], lexer)


class WhenParsingSyslog(unittest.TestCase):

    def setUp(self):
        self.lexer = SyslogLexer()
        self.buffer = bytearray(2048)

    def test_read_octet_count(self):
        map(hand_to_lexer, chunk(self.lexer, HAPPY_PATH_MESSAGE, 4))
        self.assertEqual(259, self.lexer.remaining())

    def test_read_priority(self):
        map(hand_to_lexer, chunk(self.lexer, HAPPY_PATH_MESSAGE, 8))
        self.assertTrue(self.lexer.has_token())
        self.assertEqual(255, self.lexer.remaining())
        self.assertEqual('46', str(self.lexer.get_token()))

    def test_read_version(self):
        map(hand_to_lexer, chunk(self.lexer, HAPPY_PATH_MESSAGE, 10))
        self.assertTrue(self.lexer.has_token())
        self.assertEqual(253, self.lexer.remaining())
        self.assertEqual('1', str(self.lexer.get_token()))

    def test_read_version(self):
        map(hand_to_lexer, chunk(self.lexer, HAPPY_PATH_MESSAGE, 43))
        self.assertTrue(self.lexer.has_token())
        self.assertEqual(220, self.lexer.remaining())
        self.assertEqual('2012-12-11T15:48:23.217459-06:00',
            str(self.lexer.get_token()))

    def test_read_hostname(self):
        map(hand_to_lexer, chunk(self.lexer, HAPPY_PATH_MESSAGE, 49))
        self.assertTrue(self.lexer.has_token())
        self.assertEqual(214, self.lexer.remaining())
        self.assertEqual('tohru', str(self.lexer.get_token()))

    def test_read_appname(self):
        map(hand_to_lexer, chunk(self.lexer, HAPPY_PATH_MESSAGE, 58))
        self.assertTrue(self.lexer.has_token())
        self.assertEqual(205, self.lexer.remaining())
        self.assertEqual('rsyslogd', str(self.lexer.get_token()))

    def test_read_processid(self):
        map(hand_to_lexer, chunk(self.lexer, HAPPY_PATH_MESSAGE, 64))
        self.assertTrue(self.lexer.has_token())
        self.assertEqual(199, self.lexer.remaining())
        self.assertEqual('6611', str(self.lexer.get_token()))

    def test_read_messageid(self):
        map(hand_to_lexer, chunk(self.lexer, HAPPY_PATH_MESSAGE, 69))
        self.assertTrue(self.lexer.has_token())
        self.assertEqual(194, self.lexer.remaining())
        self.assertEqual('12512', str(self.lexer.get_token()))

    def test_sd_element_name(self):
        map(hand_to_lexer, chunk(self.lexer, HAPPY_PATH_MESSAGE, 79))
        self.assertTrue(self.lexer.has_token())
        self.assertEqual(184, self.lexer.remaining())
        self.assertEqual('origin_1', str(self.lexer.get_token()))
        self.lexer.reset()
        map(hand_to_lexer, chunk(self.lexer, HAPPY_PATH_MESSAGE, 174))
        self.assertTrue(self.lexer.has_token())
        self.assertEqual(89, self.lexer.remaining())
        self.assertEqual('origin_2', str(self.lexer.get_token()))

    def test_sd_field_name(self):
        map(hand_to_lexer, chunk(self.lexer, HAPPY_PATH_MESSAGE, 88))
        self.assertTrue(self.lexer.has_token())
        self.assertEqual(175, self.lexer.remaining())
        self.assertEqual('software', str(self.lexer.get_token()))

    def test_sd_field_value(self):
        map(hand_to_lexer, chunk(self.lexer, HAPPY_PATH_MESSAGE, 98))
        self.assertTrue(self.lexer.has_token())
        self.assertEqual(165, self.lexer.remaining())
        self.assertEqual('rsyslogd', str(self.lexer.get_token()))

    def test_sd_element(self):
        map(hand_to_lexer, chunk(self.lexer, HAPPY_PATH_MESSAGE, 79))
        self.assertTrue(self.lexer.has_token())
        self.assertEqual('origin_1', str(self.lexer.get_token()))
        read_into(HAPPY_PATH_MESSAGE[79:88], 0, 9, self.lexer)
        self.assertTrue(self.lexer.has_token())
        self.assertEqual('software', str(self.lexer.get_token()))
        read_into(HAPPY_PATH_MESSAGE[88:98], 0, 10, self.lexer)
        self.assertTrue(self.lexer.has_token())
        self.assertEqual('rsyslogd', str(self.lexer.get_token()))
        read_into(HAPPY_PATH_MESSAGE[98:109], 0, 11, self.lexer)
        self.assertTrue(self.lexer.has_token())
        self.assertEqual('swVersion', str(self.lexer.get_token()))
        read_into(HAPPY_PATH_MESSAGE[109:116], 0, 7, self.lexer)
        self.assertTrue(self.lexer.has_token())
        self.assertEqual('7.2.2', str(self.lexer.get_token()))
        read_into(HAPPY_PATH_MESSAGE[116:124], 0, 8, self.lexer)
        self.assertTrue(self.lexer.has_token())
        self.assertEqual('x-pid', str(self.lexer.get_token()))
        read_into(HAPPY_PATH_MESSAGE[124:131], 0, 7, self.lexer)
        self.assertTrue(self.lexer.has_token())
        self.assertEqual('12297', str(self.lexer.get_token()))
        read_into(HAPPY_PATH_MESSAGE[131:139], 0, 8, self.lexer)
        self.assertTrue(self.lexer.has_token())
        self.assertEqual('x-info', str(self.lexer.get_token()))
        read_into(HAPPY_PATH_MESSAGE[139:162], 0, 23, self.lexer)
        self.assertTrue(self.lexer.has_token())
        self.assertEqual('http://www.rsyslog.com', self.lexer.get_token())
        read_into(HAPPY_PATH_MESSAGE[162:174], 0, 12, self.lexer)
        self.assertTrue(self.lexer.has_token())
        self.assertEqual('origin_2', str(self.lexer.get_token()))

    def test_message(self):
        map(hand_to_lexer, chunk(self.lexer, HAPPY_PATH_MESSAGE, len(HAPPY_PATH_MESSAGE)))
        self.assertTrue(self.lexer.has_token())
        self.assertEqual(0, self.lexer.remaining())
        self.assertEqual('start', str(self.lexer.get_token()))
        map(hand_to_lexer, chunk(self.lexer, HAPPY_PATH_MESSAGE, len(HAPPY_PATH_MESSAGE)))
        self.assertTrue(self.lexer.has_token())
        self.assertEqual(0, self.lexer.remaining())
        self.assertEqual('start', str(self.lexer.get_token()))


def performance(duration=10, print_output=True):
    lexer = SyslogLexer()
    data_length = len(HAPPY_PATH_MESSAGE)
    runs = 0
    then = time.time()
    while time.time() - then < duration:
        map(hand_to_lexer, chunk(lexer, HAPPY_PATH_MESSAGE, 263))
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
