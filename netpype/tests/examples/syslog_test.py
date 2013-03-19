import unittest

from netpype.examples.syslog import SyslogLexer, lexer_states

HAPPY_PATH_MESSAGE = (b'123 <46>1 2012-12-11T15:48:23.217459-06:00 tohru ' +
                      b'rsyslogd 6611 12512  [origin software="rsyslogd" ' +
                      b'swVersion="7.2.2" x-pid="12297" ' +
                      b'x-info="http://www.rsyslog.com"] ' +
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
        self.lexer.on_read(HAPPY_PATH_MESSAGE[:4])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[4:10])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[10:43])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[43:49])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[49:58])
        self.assertEqual(lexer_states.READ_PROCESSID, self.lexer.get_state())

    def test_processid(self):
        self.lexer.on_connect('localhost')
        self.lexer.on_read(HAPPY_PATH_MESSAGE[:4])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[4:10])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[10:43])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[43:49])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[49:58])
        self.assertEqual(lexer_states.READ_PROCESSID, self.lexer.get_state())

    def test_appname(self):
        self.lexer.on_connect('localhost')
        self.lexer.on_read(HAPPY_PATH_MESSAGE[:4])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[4:10])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[10:43])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[43:49])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[49:58])
        self.lexer.on_read(HAPPY_PATH_MESSAGE[58:63])
        self.assertEqual(lexer_states.READ_MESSAGEID, self.lexer.get_state())

if __name__ == '__main__':
    unittest.main()
