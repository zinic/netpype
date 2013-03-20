import time
import netpype.env as env

from netpype.selector import events as selection_events, new_server
from netpype.channel import SocketINet4Address, CyclicBuffer
from netpype.channel import NetworkEventHandler, PipelineFactory


_LOG = env.get_logger('netpype.examples.simple')
_MAX_BYTES = 536870912

_SPACE_ORD = ord(' ')
_QUOTE_ORD = ord('"')
_EQUALS_ORD = ord('=')
_CLOSE_ANGLE_BRACKET_ORD = ord('>')
_OPEN_BRACKET_ORD = ord('[')
_CLOSE_BRACKET_ORD = ord(']')


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

    def __init__(self, message_length):
        self.structured_data = dict()
        self.message_length = message_length

    def sd_element(self, name_bytes):
        name = str(name_bytes)
        sd_element = self.structured_data.get(name)
        if sd_element is None:
            sd_element = StructuredData(name)
            self.structured_data[name] = sd_element
        return sd_element


class StructuredData(object):
    
    def __init__(self, name):
        self.fields = dict()
    
    def sd_field(self, name_bytes):
        name = str(name_bytes)
        sd_field = self.fields.get(name)
        if sd_field is None:
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
        self._accumulator.put(message, 0)
        while self._accumulator.available() > 0 and self.parse_next():
            pass

    def on_write(self, message):
        _LOG.info('Requesting close.')
        return (selection_events.REQUEST_CLOSE, None)

    def on_close(self, message):
        _LOG.info('Closing connection to {}'.format(message))

    def _skip(self, length):
        self._octet_count -= self._accumulator.skip(length)

    def parse_next(self):
        read = -1
        if self._state == lexer_states.START:
            self._octet_count = 0
            self._state = lexer_states.READ_OCTET
            self._structured_data = None
            self._sd_field = None
        if self._state == lexer_states.READ_OCTET:
            read = self._accumulator.get_until(
                delim=_SPACE_ORD,
                data=self._lookaside,
                limit=9)
            if read > -1:
                # Set octet count
                self._octet_count = int(self._lookaside[:read])
                self._message = SyslogMessage(self._octet_count)
                # Skip the following space
                self._skip(1)
                self._state = lexer_states.READ_PRI
        elif self._state == lexer_states.READ_PRI:
            read = self._accumulator.get_until(
                delim=_CLOSE_ANGLE_BRACKET_ORD,
                data=self._lookaside,
                limit=5)
            if read > -1:
                # Skip the following closing angle bracket
                self._skip(1)
                self._state = lexer_states.READ_VERSION
                self._message.priority = self._lookaside[1:read]
        elif self._state == lexer_states.READ_VERSION:
            read = self._accumulator.get_until(
                delim=_SPACE_ORD,
                data=self._lookaside,
                limit=2)
            if read > -1:
                # Skip the following space
                self._skip(1)
                self._state = lexer_states.READ_TIMESTAMP
                self._message.version = self._lookaside[:read]
        elif self._state == lexer_states.READ_TIMESTAMP:
            read = self._accumulator.get_until(
                delim=_SPACE_ORD,
                data=self._lookaside,
                limit=48)
            if read > -1:
                # Skip the following space
                self._skip(1)
                self._state = lexer_states.READ_HOSTNAME
                self._message.timestamp = self._lookaside[:read]
        elif self._state == lexer_states.READ_HOSTNAME:
            read = self._accumulator.get_until(
                delim=_SPACE_ORD,
                data=self._lookaside,
                limit=255)
            if read > -1:
                # Skip the following space
                self._skip(1)
                self._state = lexer_states.READ_APPNAME
                self._message.hostname = self._lookaside[:read]
        elif self._state == lexer_states.READ_APPNAME:
            read = self._accumulator.get_until(
                delim=_SPACE_ORD,
                data=self._lookaside,
                limit=48)
            if read > -1:
                # Skip the following space
                self._skip(1)
                self._state = lexer_states.READ_PROCESSID
                self._message.appname = self._lookaside[:read]
        elif self._state == lexer_states.READ_PROCESSID:
            read = self._accumulator.get_until(
                delim=_SPACE_ORD,
                data=self._lookaside,
                limit=128)
            if read > -1:
                # Skip the following space
                self._skip(1)
                self._state = lexer_states.READ_MESSAGEID
                self._message.processid = self._lookaside[:read]
        elif self._state == lexer_states.READ_MESSAGEID:
            read = self._accumulator.get_until(
                delim=_SPACE_ORD,
                data=self._lookaside,
                limit=32)
            if read > -1:
                # Skip the following space
                self._skip(1)
                self._state = lexer_states.READ_SD_ELEMENT
                self._message.messageid = self._lookaside[:read]
        elif self._state == lexer_states.READ_SD_ELEMENT:
            read = self._accumulator.get(
                data=self._lookaside,
                length=1)
            if read > 0:
                potential_delim = self._lookaside[0]
                if potential_delim == _SPACE_ORD:
                    self._state = lexer_states.READ_MESSAGE
                elif potential_delim == _OPEN_BRACKET_ORD:
                    self._state = lexer_states.READ_SD_ELEMENT_NAME
                else:
                    raise Exception('Unexpected: {} - {}'.format(
                        chr(potential_delim), potential_delim))
        elif self._state == lexer_states.READ_SD_ELEMENT_NAME:
            read = self._accumulator.get_until(
                delim=_SPACE_ORD,
                data=self._lookaside,
                limit=32)
            if read > -1:
                # Skip the following space
                self._skip(1)
                self._structured_data = self._message.sd_element(
                    self._lookaside[:read])
                self._state = lexer_states.READ_SD_FIELD_NAME
        elif self._state == lexer_states.READ_SD_FIELD_NAME:
            read = self._accumulator.get_until(
                delim=_EQUALS_ORD,
                data=self._lookaside,
                limit=32)
            if read > -1:
                # Skip the assignment symbol
                self._skip(1)
                self._sd_field = self._structured_data.sd_field(
                    self._lookaside[:read])             
                self._state = lexer_states.READ_SD_VALUE_START
        elif self._state == lexer_states.READ_SD_VALUE_START:
            read = self._accumulator.get_until(
                delim=_QUOTE_ORD,
                data=self._lookaside,
                limit=32)
            if read > -1:
                # Skip the quote
                self._skip(1)
                self._state = lexer_states.READ_SD_VALUE_CONTENT
        elif self._state == lexer_states.READ_SD_VALUE_CONTENT:
            read = self._accumulator.get_until(
                delim=_QUOTE_ORD,
                data=self._lookaside,
                limit=255)
            if read > -1:
                # Skip the quote
                self._skip(1)
                self._sd_field.value = self._lookaside[:read]
                self._state = lexer_states.READ_SD_NEXT_FIELD_OR_END
        elif self._state == lexer_states.READ_SD_NEXT_FIELD_OR_END:
            read = self._accumulator.get(
                data=self._lookaside,
                length=1)
            if read > 0:
                potential_delim = self._lookaside[0]
                if potential_delim == _SPACE_ORD:
                    self._state = lexer_states.READ_SD_FIELD_NAME
                elif potential_delim == _CLOSE_BRACKET_ORD:
                    self._state = lexer_states.READ_SD_ELEMENT
        elif self._state == lexer_states.READ_MESSAGE:
            read = self._accumulator.get(
                data=self._lookaside,
                length=self._octet_count)

            if read > 0 and self._octet_count - read == 0:
                self._state = lexer_states.START

        # If something was read, there might be more
        if read > 0:
            self._octet_count -= read
            if self._octet_count < 0:
                raise Exception
            return True
        else:
            return False


class BasicPipelineFactory(PipelineFactory):

    def upstream_pipeline(self):
        return [BasicHandler()]

    def downstream_pipeline(self):
        return [BasicHandler()]


def go():
    socket_info = SocketINet4Address('127.0.0.1', 8080)
    server = new_server(socket_info, BasicPipelineFactory())
    server.start()
    time.sleep(10000)
    server.stop()


if __name__ == '__main__':
    go()
