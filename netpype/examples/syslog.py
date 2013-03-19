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
_OPEN_BRACKET_ORD = ord('[')
_CLOSE_BRACKET_ORD = ord(']')


class LexerState(object):
    START = 0
    READ_OCTET = 1
    READ_PRI = 2
    READ_TIMESTAMP = 3
    READ_HOSTNAME = 4
    READ_APPNAME = 5
    READ_PROCESSID = 6
    READ_MESSAGEID = 7
    READ_SD_ELEMENT = 8
    READ_SD_ELEMENT_NAME = 9
    READ_SD_FIELD_NAME = 10
    READ_SD_VALUE_START = 11
    READ_SD_VALUE_CONTENT = 12
    READ_SD_NEXT_FIELD_OR_END = 13
    READ_MESSAGE = 14

lexer_states = LexerState()


class SyslogLexer(NetworkEventHandler):

    def __init__(self):
        self._accumulator = CyclicBuffer(size_hint=1024)
        self._lookaside = bytearray(1024)
        self._state = lexer_states.START

    def get_state(self):
        return self._state

    def on_connect(self, message):
        _LOG.info('Syslog client connected @ {}.'.format(message))
        self._state = lexer_states.READ_OCTET
        return (selection_events.REQUEST_READ, None)

    def on_read(self, message):
        # Load into our accumulator
        self._accumulator.put(message, 0)
        while self.parse_next():
            pass

    def on_write(self, message):
        _LOG.info('Requesting close.')
        return (selection_events.REQUEST_CLOSE, None)

    def on_close(self, message):
        _LOG.info('Closing connection to {}'.format(message))

    def parse_next(self):
        if self._accumulator.available() > 0:
            if self._state == lexer_states.READ_OCTET:
                read = self._accumulator.get_until(
                    delim=_SPACE_ORD,
                    data=self._lookaside,
                    limit=9)
                if read > 0:
                    # Skip the following space
                    self._accumulator.skip(1)
                    self._state = lexer_states.READ_PRI
                    value = self._lookaside[:read]
                    return True
            elif self._state == lexer_states.READ_PRI:
                read = self._accumulator.get_until(
                    delim=_SPACE_ORD,
                    data=self._lookaside,
                    limit=7)
                if read > 0:
                    # Skip the following space
                    self._accumulator.skip(1)
                    self._state = lexer_states.READ_TIMESTAMP
                    value = self._lookaside[:read]
                    return True
            elif self._state == lexer_states.READ_TIMESTAMP:
                read = self._accumulator.get_until(
                    delim=_SPACE_ORD,
                    data=self._lookaside,
                    limit=48)
                if read > 0:
                    # Skip the following space
                    self._accumulator.skip(1)
                    self._state = lexer_states.READ_HOSTNAME
                    value = self._lookaside[:read]
                    return True
            elif self._state == lexer_states.READ_HOSTNAME:
                read = self._accumulator.get_until(
                    delim=_SPACE_ORD,
                    data=self._lookaside,
                    limit=255)
                if read > 0:
                    # Skip the following space
                    self._accumulator.skip(1)
                    self._state = lexer_states.READ_APPNAME
                    value = self._lookaside[:read]
                    return True
            elif self._state == lexer_states.READ_APPNAME:
                read = self._accumulator.get_until(
                    delim=_SPACE_ORD,
                    data=self._lookaside,
                    limit=48)
                if read > 0:
                    # Skip the following space
                    self._accumulator.skip(1)
                    self._state = lexer_states.READ_PROCESSID
                    value = self._lookaside[:read]
                    return True
            elif self._state == lexer_states.READ_PROCESSID:
                read = self._accumulator.get_until(
                    delim=_SPACE_ORD,
                    data=self._lookaside,
                    limit=128)
                if read > 0:
                    # Skip the following space
                    self._accumulator.skip(1)
                    self._state = lexer_states.READ_MESSAGEID
                    value = self._lookaside[:read]
                    return True
            elif self._state == lexer_states.READ_MESSAGEID:
                read = self._accumulator.get_until(
                    delim=_SPACE_ORD,
                    data=self._lookaside,
                    limit=32)
                if read > 0:
                    # Skip the following space
                    self._accumulator.skip(1)
                    self._state = lexer_states.READ_SD_ELEMENT
                    value = self._lookaside[:read]
                    return True
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
                    return True
            elif self._state == lexer_states.READ_SD_ELEMENT_NAME:
                read = self._accumulator.get_until(
                    delim=_SPACE_ORD,
                    data=self._lookaside,
                    limit=32)
                if read > 0:
                    # Skip the following space
                    self._accumulator.skip(1)
                    self._state = lexer_states.READ_SD_FIELD_NAME
                    return True
            elif self._state == lexer_states.READ_SD_FIELD_NAME:
                read = self._accumulator.get_until(
                    delim=_EQUALS_ORD,
                    data=self._lookaside,
                    limit=32)
                if read > 0:
                    # Skip the assignment symbol
                    self._accumulator.skip(1)
                    self._state = lexer_states.READ_SD_VALUE_START
                    return True
            elif self._state == lexer_states.READ_SD_VALUE_START:
                read = self._accumulator.get(
                    data=self._lookaside,
                    length=1)

                if read > 0:
                    potential_delim = self._lookaside[0]
                    if potential_delim == _QUOTE_ORD:
                        self._state = lexer_states.READ_SD_VALUE_CONTENT
                    else:
                        raise Exception('Unexpected: {} - {}'.format(
                            chr(potential_delim), potential_delim))
                    return True
            elif self._state == lexer_states.READ_SD_VALUE_CONTENT:
                read = self._accumulator.get_until(
                    delim=_QUOTE_ORD,
                    data=self._lookaside,
                    limit=255)
                if read > 0:
                    # Skip the quote
                    self._accumulator.skip(1)
                    self._state = lexer_states.READ_SD_NEXT_FIELD_OR_END
                    return True
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
                    return True
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
