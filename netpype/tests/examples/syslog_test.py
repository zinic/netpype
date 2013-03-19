import unittest

from netpype.examples.syslog import SyslogLexer, lexer_states


HAPPY_PATH_MESSAGE = (b'123 <46>1 2012-12-11T15:48:23.217459-06:00 tohru ' +
                      b'rsyslogd 6611 12512 [origin software="rsyslogd" ' +
                      b'swVersion="7.2.2" x-pid="12297" ' +
                      b'x-info="http://www.rsyslog.com"]' +
                      b'[origin software="rsyslogd" swVersion="7.2.2" ' +
                      b'x-pid="12297" x-info="http://www.rsyslog.com"] ' +
                      b'start')


class WhenLexingSyslogHead(unittest.TestCase):

    def setUp(self):
        self.lexer = SyslogLexer()

    def test_connect(self):
        self.lexer.on_connect('localhost')
        self.assertEqual(lexer_states.READ_OCTET, self.lexer.get_state())

    def test_octet_count(self):
        self.lexer.on_connect('localhost')
        self.lexer.on_read(HAPPY_PATH_MESSAGE[:4])
        self.assertEqual(lexer_states.READ_PRI, self.lexer.get_state())

    def test_octect_count_too_long(self):
        self.lexer.on_connect('localhost')
        self.assertRaises(
            Exception,
            self.lexer.on_read,
            b'5193859319513958131234')

    def test_pri(self):
        self.lexer.on_connect('localhost')
        self.lexer.on_read(HAPPY_PATH_MESSAGE[:4])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[4:10])
        self.assertEqual(lexer_states.READ_TIMESTAMP, self.lexer.get_state())

    def test_timestamp(self):
        self.lexer.on_connect('localhost')
        self.lexer.on_read(HAPPY_PATH_MESSAGE[:4])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[4:10])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[10:43])
        self.assertEqual(lexer_states.READ_HOSTNAME, self.lexer.get_state())

    def test_hostname(self):
        self.lexer.on_connect('localhost')
        self.lexer.on_read(HAPPY_PATH_MESSAGE[:4])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[4:10])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[10:43])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[43:49])
        self.assertEqual(lexer_states.READ_APPNAME, self.lexer.get_state())

    def test_appname(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 58))
        self.assertEqual(lexer_states.READ_PROCESSID, self.lexer.get_state())

    def test_processid(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 64))
        self.assertEqual(lexer_states.READ_MESSAGEID, self.lexer.get_state())

    def test_messageid(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 69))
        self.assertEqual(
            lexer_states.READ_SD_ELEMENT, self.lexer.get_state())

    def test_sd_element(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 70))
        self.assertEqual(
            lexer_states.READ_SD_ELEMENT_NAME, self.lexer.get_state())

    def test_sd_element_name(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 77))
        self.assertEqual(
            lexer_states.READ_SD_FIELD_NAME, self.lexer.get_state())

    def test_sd_field_name(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 86))
        self.assertEqual(
            lexer_states.READ_SD_VALUE_START, self.lexer.get_state())

    def test_sd_field_value_start(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 87))
        self.assertEqual(
            lexer_states.READ_SD_VALUE_CONTENT, self.lexer.get_state())

    def test_sd_field_value_content(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read, chunk(HAPPY_PATH_MESSAGE, 96))
        self.assertEqual(
            lexer_states.READ_SD_NEXT_FIELD_OR_END, self.lexer.get_state())

    def test_msg(self):
        self.lexer.on_connect('localhost')
        map(self.lexer.on_read,
            chunk(HAPPY_PATH_MESSAGE, len(HAPPY_PATH_MESSAGE)))
        self.assertEqual(
            lexer_states.READ_MESSAGE, self.lexer.get_state())


def chunk(data, limit):
    index = 0
    while index < limit:
        next_index = index + 10
        end_index = next_index if next_index < limit else limit
        yield data[index:end_index]
        index = end_index


if __name__ == '__main__':
    unittest.main()
