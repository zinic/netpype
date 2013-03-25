import time
import netpype.env as env

from netpype.selector import events as selection_events, new_server
from netpype.channel import SocketINet4Address
from netpype.channel import NetworkEventHandler, PipelineFactory

try:
    from netpype.cutil import CyclicBuffer
except ImportError:
    from netpype.channel import CyclicBuffer

# Delimeter constants
_SPACE = ' '
_QUOTE = '"'
_EQUALS = '='
_CLOSE_ANGLE_BRACKET = '>'
_OPEN_BRACKET = '['
_CLOSE_BRACKET = ']'

# Ordinal values
_SPACE_ORD = ord(_SPACE)
_OPEN_BRACKET_ORD = ord(_OPEN_BRACKET)
_CLOSE_BRACKET_ORD = ord(_CLOSE_BRACKET)

_LOG = env.get_logger('netpype.examples.simple')
_MAX_BYTES = 536870912


class LexerState(object):
    START = 0
    READ_OCTET = 1
    READ_PRI = 2
    READ_VERSION = 3
    READ_TIMESTAMP = 4
    READ_HOSTNAME = 5
    READ_APPNAME = 6
    READ_PROCESSID = 7
    READ_MESSAGEID = 8
    READ_SD_ELEMENT = 9
    READ_SD_ELEMENT_NAME = 10
    READ_SD_FIELD_NAME = 11
    READ_SD_VALUE_START = 12
    READ_SD_VALUE_CONTENT = 13
    READ_SD_NEXT_FIELD_OR_END = 14
    READ_MESSAGE = 15

lexer_states = LexerState()


class SyslogMessage(object):

    def __init__(self):
        self.structured_data = dict()

    def sd_element(self, name_bytes):
        name = str(name_bytes)
        sd_element = StructuredData(name)
        self.structured_data[name] = sd_element
        return sd_element


class StructuredData(object):
    
    def __init__(self, name):
        self.fields = dict()
    
    def sd_field(self, name_bytes):
        name = str(name_bytes)
        sd_field = StructuredDataField(name)
        self.fields[name] = sd_field
        return sd_field


class StructuredDataField(object):
    
    def __init__(self, name):
        self.name = name
        self.value = None


class SyslogLexer(NetworkEventHandler):

    def __init__(self):
        self._accumulator = CyclicBuffer(size_hint=1024)
        self._lookaside = bytearray(1024)
        self._state = lexer_states.START

    def get_message(self):
        return self._message

    def get_state(self):
        return self._state

    def on_connect(self, message):
        _LOG.info('Syslog client connected @ {}.'.format(message))
        return (selection_events.REQUEST_READ, None)

    def on_read(self, message):
        # Load into our accumulator
        self._accumulator.put(message)
        while self._accumulator.available > 0 and self.parse_next():
            pass
        
    def on_write(self, message):
        _LOG.info('Requesting close.')
        return (selection_events.REQUEST_CLOSE, None)

    def on_close(self, message):
        _LOG.info('Closing connection to {}'.format(message))

    def _get_until(self, delimeter, read_limit):
        read = self._accumulator.get_until(
            delimeter,
            self._lookaside,
            0,
            read_limit)
        if read > -1:
            # Skip the following delim and mark what we've read
            self._accumulator.skip(1)
            self._octet_count -= read + 1
            self._read_offset = read
        return read

    def _next_token(self, offset=0):
        return self._lookaside[offset:self._read_offset]

    def parse_next(self):
        if self._state == lexer_states.START:
            self._octet_count = 0
            self._read_offset = 0
            self._state = lexer_states.READ_OCTET
            self._structured_data = None
            self._sd_field = None
        
        if self._state == lexer_states.READ_OCTET:
            if self._get_until(_SPACE, 9) > -1:
                self._octet_count += int(self._next_token())
                self._message = SyslogMessage()
                self._state = lexer_states.READ_PRI
                return True
        elif self._state == lexer_states.READ_PRI:
            if self._get_until(_CLOSE_ANGLE_BRACKET, 5) > -1:
                self._state = lexer_states.READ_VERSION
                self._message.priority = self._next_token(1)
                return True
        elif self._state == lexer_states.READ_VERSION:
            if self._get_until(_SPACE, 2) > -1:
                self._state = lexer_states.READ_TIMESTAMP
                self._message.version = self._next_token()
                return True
        elif self._state == lexer_states.READ_TIMESTAMP:
            if self._get_until(_SPACE, 48) > -1:
                self._state = lexer_states.READ_HOSTNAME
                self._message.timestamp = self._next_token()
                return True
        elif self._state == lexer_states.READ_HOSTNAME:
            if self._get_until(_SPACE, 255) > -1:
                self._state = lexer_states.READ_APPNAME
                self._message.hostname = self._next_token()
                return True
        elif self._state == lexer_states.READ_APPNAME:
            if self._get_until(_SPACE, 48) > -1:
                self._state = lexer_states.READ_PROCESSID
                self._message.appname = self._next_token()
                return True
        elif self._state == lexer_states.READ_PROCESSID:
            if self._get_until(_SPACE, 128) > -1:
                self._state = lexer_states.READ_MESSAGEID
                self._message.processid = self._next_token()
                return True
        elif self._state == lexer_states.READ_MESSAGEID:
            if self._get_until(_SPACE, 32) > -1:
                self._state = lexer_states.READ_SD_ELEMENT
                self._message.messageid = self._next_token()
                return True
        elif self._state == lexer_states.READ_SD_ELEMENT:
            read = self._accumulator.get(
                data=self._lookaside,
                length=1)
            if read > 0:
                self._octet_count -= read
                potential_delim = self._lookaside[0]
                if potential_delim == _SPACE_ORD:
                    self._state = lexer_states.READ_MESSAGE
                elif potential_delim == _OPEN_BRACKET_ORD:
                    self._state = lexer_states.READ_SD_ELEMENT_NAME
                else:
                    raise Exception('Unexpected delimeter: {} - {}'.format(
                        chr(potential_delim), potential_delim))
                return True
        elif self._state == lexer_states.READ_SD_ELEMENT_NAME:
            if self._get_until(_SPACE, 32) > -1:
                self._structured_data = self._message.sd_element(
                    self._next_token())
                self._state = lexer_states.READ_SD_FIELD_NAME
                return True
        elif self._state == lexer_states.READ_SD_FIELD_NAME:
            if self._get_until(_EQUALS, 32) > -1:
                self._sd_field = self._structured_data.sd_field(
                    self._next_token())             
                self._state = lexer_states.READ_SD_VALUE_START
                return True
        elif self._state == lexer_states.READ_SD_VALUE_START:
            if self._get_until(_QUOTE, 32) > -1:
                self._state = lexer_states.READ_SD_VALUE_CONTENT
                return True
        elif self._state == lexer_states.READ_SD_VALUE_CONTENT:
            if self._get_until(_QUOTE, 255) > -1:
                self._sd_field.value = self._next_token()
                self._state = lexer_states.READ_SD_NEXT_FIELD_OR_END
                return True
        elif self._state == lexer_states.READ_SD_NEXT_FIELD_OR_END:
            read = self._accumulator.get(
                data=self._lookaside,
                length=1)
            if read > 0:
                self._octet_count -= read
                potential_delim = self._lookaside[0]
                if potential_delim == _SPACE_ORD:
                    self._state = lexer_states.READ_SD_FIELD_NAME
                elif potential_delim == _CLOSE_BRACKET_ORD:
                    self._state = lexer_states.READ_SD_ELEMENT
                return True
        elif self._state == lexer_states.READ_MESSAGE:
            read = self._accumulator.get(
                data=self._lookaside,
                length=self._octet_count)
            if read > 0:
                self._octet_count -= read
                if self._octet_count == 0:
                    self._state = lexer_states.START
        return False


class EmptyHandler(NetworkEventHandler):
    pass

class SyslogPipelineFactory(PipelineFactory):

    def upstream_pipeline(self):
        return [SyslogLexer()]

    def downstream_pipeline(self):
        return [SyslogLexer()]


def go():
    socket_info = SocketINet4Address('127.0.0.1', 5140)
    server = new_server(socket_info, SyslogPipelineFactory())
    server.start()
    time.sleep(10000)
    server.stop()


if __name__ == '__main__':
    go()
