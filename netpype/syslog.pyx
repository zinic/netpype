from libc.stdlib cimport malloc, free
from netpype.cutil cimport CyclicBuffer
from cython import array

cdef enum LexerStates:
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

_MAX_BYTES = 536870912

cdef struct StructuredDataField:
    char** names, values
    int num_fields

cdef struct StructuredData:
    char* name
    StructuredDataField* fields

cdef class SyslogMessage(object):
    
    cdef StructuredData* _sd_elements
    cdef int _num_elements

    def __init__(self):
        _sd_elements = <StructuredData*> malloc(sizeof(StructuredData) * 32)

    def __dealloc__(self):
        cdef int index = 0
        while index < self._num_elements:
            free(_sd_elements[index].fields)
        free(_sd_elements)
        
    cdef sd_element(self, char* name):
        cdef StructuredData sde
        sde.name = name
        sde.fields
        self._sd_elements[self.num_elements] = sde
        self._num_elements += 1
        

cdef class SyslogLexer(object):

    cdef CyclicBuffer _accumulator
    cdef char* _lookaside
    cdef int _octet_count, _read_offset, _state
    
    def __cinit__(self, int size_hint=1024):
        self._accumulator = CyclicBuffer(size_hint)
        self._lookaside = <char*> malloc(sizeof(char) * size_hint) 
        self._state = START

    def get_message(self):
        return self._message

    def get_state(self):
        return self._state

    def on_read(self, message):
        # Load into our accumulator
        self._accumulator.put(message)
        while self._accumulator.available() > 0 and self.parse_next():
            pass
        
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
        if self._state == START:
            self._octet_count = 0
            self._read_offset = 0
            self._state = READ_OCTET
            self._structured_data = None
            self._sd_field = None
        
        if self._state == READ_OCTET:
            if self._get_until(_SPACE, 9) > -1:
                self._octet_count += int(self._next_token())
                self._message = SyslogMessage()
                self._state = READ_PRI
                return True
        elif self._state == READ_PRI:
            if self._get_until(_CLOSE_ANGLE_BRACKET, 5) > -1:
                self._state = READ_VERSION
                self._message.priority = self._next_token(1)
                return True
        elif self._state == READ_VERSION:
            if self._get_until(_SPACE, 2) > -1:
                self._state = READ_TIMESTAMP
                self._message.version = self._next_token()
                return True
        elif self._state == READ_TIMESTAMP:
            if self._get_until(_SPACE, 48) > -1:
                self._state = READ_HOSTNAME
                self._message.timestamp = self._next_token()
                return True
        elif self._state == READ_HOSTNAME:
            if self._get_until(_SPACE, 255) > -1:
                self._state = READ_APPNAME
                self._message.hostname = self._next_token()
                return True
        elif self._state == READ_APPNAME:
            if self._get_until(_SPACE, 48) > -1:
                self._state = READ_PROCESSID
                self._message.appname = self._next_token()
                return True
        elif self._state == READ_PROCESSID:
            if self._get_until(_SPACE, 128) > -1:
                self._state = READ_MESSAGEID
                self._message.processid = self._next_token()
                return True
        elif self._state == READ_MESSAGEID:
            if self._get_until(_SPACE, 32) > -1:
                self._state = READ_SD_ELEMENT
                self._message.messageid = self._next_token()
                return True
        elif self._state == READ_SD_ELEMENT:
            read = self._accumulator.get(
                data=self._lookaside,
                length=1)
            if read > 0:
                self._octet_count -= read
                potential_delim = self._lookaside[0]
                if potential_delim == _SPACE_ORD:
                    self._state = READ_MESSAGE
                elif potential_delim == _OPEN_BRACKET_ORD:
                    self._state = READ_SD_ELEMENT_NAME
                else:
                    raise Exception('Unexpected delimeter: {} - {}'.format(
                        chr(potential_delim), potential_delim))
                return True
        elif self._state == READ_SD_ELEMENT_NAME:
            if self._get_until(_SPACE, 32) > -1:
                self._structured_data = self._message.sd_element(
                    self._next_token())
                self._state = READ_SD_FIELD_NAME
                return True
        elif self._state == READ_SD_FIELD_NAME:
            if self._get_until(_EQUALS, 32) > -1:
                self._sd_field = self._structured_data.sd_field(
                    self._next_token())             
                self._state = READ_SD_VALUE_START
                return True
        elif self._state == READ_SD_VALUE_START:
            if self._get_until(_QUOTE, 32) > -1:
                self._state = READ_SD_VALUE_CONTENT
                return True
        elif self._state == READ_SD_VALUE_CONTENT:
            if self._get_until(_QUOTE, 255) > -1:
                self._sd_field.value = self._next_token()
                self._state = READ_SD_NEXT_FIELD_OR_END
                return True
        elif self._state == READ_SD_NEXT_FIELD_OR_END:
            read = self._accumulator.get(
                data=self._lookaside,
                length=1)
            if read > 0:
                self._octet_count -= read
                potential_delim = self._lookaside[0]
                if potential_delim == _SPACE_ORD:
                    self._state = READ_SD_FIELD_NAME
                elif potential_delim == _CLOSE_BRACKET_ORD:
                    self._state = READ_SD_ELEMENT
                return True
        elif self._state == READ_MESSAGE:
            read = self._accumulator.get(
                data=self._lookaside,
                length=self._octet_count)
            if read > 0:
                self._octet_count -= read
                if self._octet_count == 0:
                    self._state = START
        return False

