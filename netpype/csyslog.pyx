from libc.stdlib cimport realloc, malloc, free, atoi
from cpython cimport bool
from cython import array

cdef extern from "Python.h":
    char* PyByteArray_AsString(object bytearray) except NULL
    char* PyByteArray_AS_STRING(object bytearray) except NULL
    object PyString_FromStringAndSize(char *string, Py_ssize_t length)
    object PyByteArray_FromStringAndSize(char *string, Py_ssize_t length)
    int PyByteArray_Check(object bytearray)
    int PyByteArray_Size(object bytearray)
    int PyByteArray_GET_SIZE(object bytearray)


NO_TOKENS = 0
PRIORITY_TOKEN = 1
VERSION_TOKEN = 2
TIMESTAMP_TOKEN = 3
HOSTNAME_TOKEN = 4
APPNAME_TOKEN = 5
PROCESSID_TOKEN = 6
MESSAGEID_TOKEN = 7
SDE_NAME_TOKEN = 8
SDE_FIELD_NAME_TOKEN = 9
SDE_FIELD_VALUE_TOKEN = 10
MESSAGE_PART_TOKEN = 11


class SyslogMessageAccumulator(object):

    def on_message_part(self, message_part):
        raise NotImplementedError


class SyslogMessageHead(object):

    def __init__(self):
        self.priority = ''
        self.version = ''
        self.timestamp = ''
        self.hostname = ''
        self.appname = ''
        self.processid = ''
        self.messageid = ''
        self.sd = dict()

    def get_sd(self, name):
        return self.sd.get(name)

    def create_sd(self, sd_name):
        self.sd[sd_name] = dict()

    def add_sd_field(self, sd_name, sd_fieldname, sd_value):
        self.sd[sd_name][sd_fieldname] = sd_value


class SyslogParser():

    def __init__(self, SyslogLexer lexer, message_accumulator=None):
        self.lexer = lexer
        self.message = SyslogMessageHead()
        self.sd_name = None
        self.sd_fieldname = None

    def read(self, bytearray, offset=0, length=-1):
        if length == -1:
            length = len(bytearray)
        index = offset
        while index < length:
            self.lexer.next(bytearray[index])
            if self.lexer.has_token():
                self.handle_token()
            index += 1
        return index - offset

    def reset(self):
        self.message = SyslogMessageHead()

    def handle_token(self):
        token_type = self.lexer.token_type()

        if token_type == PRIORITY_TOKEN:
            self.message.priority = self.lexer.get_token()
        if token_type == VERSION_TOKEN:
            self.message.version = self.lexer.get_token()
        if token_type == TIMESTAMP_TOKEN:
            self.message.timestamp = self.lexer.get_token()
        if token_type == HOSTNAME_TOKEN:
            self.message.hostname = self.lexer.get_token()
        if token_type == APPNAME_TOKEN:
            self.message.appname = self.lexer.get_token()
        if token_type == PROCESSID_TOKEN:
            self.message.processid = self.lexer.get_token()
        if token_type == MESSAGEID_TOKEN:
            self.message.messageid = self.lexer.get_token()
        if token_type == SDE_NAME_TOKEN:
            self.sde_name = self.lexer.get_token()
            self.message.create_sd(self.sde_name)
        if token_type == SDE_FIELD_NAME_TOKEN:
            self.sde_field_name = self.lexer.get_token()
        if token_type == SDE_FIELD_VALUE_TOKEN:
            self.message.add_sd_field(self.sde_name, self.sde_field_name, self.lexer.get_token())


# Delimeter constants
cdef char SPACE = ' '
cdef char DASH = '-'
cdef char QUOTE = '"'
cdef char EQUALS = '='
cdef char OPEN_ANGLE_BRACKET = '<'
cdef char CLOSE_ANGLE_BRACKET = '>'
cdef char OPEN_BRACKET = '['
cdef char CLOSE_BRACKET = ']'

cdef int RFC3164_MAX_BYTES = 1024
cdef int RFC5424_MAX_BYTES = 2048

cdef int MAX_BYTES = 536870912


cdef class SyslogLexer(object):

    cdef Py_ssize_t token_length, buffer_size
    cdef int buffered_octets, octets_left, token
    cdef char *read_buffer, *token_buffer
    cdef lexer_state current_state

    def __cinit__(self, int size_hint=RFC5424_MAX_BYTES):
        self.buffer_size = size_hint
        self.read_buffer = <char*> malloc(sizeof(char) * size_hint)
        self.token_buffer = <char*> malloc(sizeof(char) * size_hint)
        self.reset()

    def __dealloc__(self):
        if self.read_buffer is not NULL:
            free(self.read_buffer)
        if self.token_buffer is not NULL:
            free(self.token_buffer)

    def reset(self):
        self._reset()

    def state(self):
        return self.current_state

    cpdef int remaining(self):
        return self.octets_left

    cpdef int token_type(self):
        return self.token

    cpdef get_token(self):
        cdef object next_token
        # Export the token info
        next_token = PyString_FromStringAndSize(self.token_buffer, self.token_length)
        # Reset the local token info
        self.token_length = 0
        # Return the token
        return next_token

    cdef int token_size(self):
        return self.token_length

    cpdef bool has_token(self):
        return self.token_length > 0

    cdef void _reset(self):
        self.current_state = OCTET
        self.buffered_octets = 0
        self.token_length = 0

    cdef void collect(self, char byte):
        self.read_buffer[self.buffered_octets] = byte
        self.buffered_octets += 1

    cdef void _copy_into(self, char *dest):
        cdef int index = 0
        while index < self.buffered_octets:
            dest[index] = self.read_buffer[index]
            index += 1
        self.buffered_octets = 0

    cdef void buffer_token(self, int token_type):
        # Swap buffers
        self.token = token_type
        cdef char *buffer_ref = self.token_buffer
        self.token_buffer = self.read_buffer
        self.read_buffer = buffer_ref
        self.token_length = self.buffered_octets
        self.buffered_octets = 0

    def next(self, char next_byte):
        if self.current_state == OCTET:
            self.read_octet(next_byte)
        else:
            self.next_msg_part(next_byte)

    cdef void next_msg_part(self, char next_byte):
        if self.current_state == PRIORITY_BEGIN:
            self.read_priority_start(next_byte)
        elif self.current_state == PRIORITY:
            self.read_token(next_byte, CLOSE_ANGLE_BRACKET, VERSION, PRIORITY_TOKEN)
        elif self.current_state == VERSION:
            self.read_token(next_byte, SPACE, TIMESTAMP, VERSION_TOKEN)
        elif self.current_state == TIMESTAMP:
            self.read_token(next_byte, SPACE, HOSTNAME, TIMESTAMP_TOKEN)
        elif self.current_state == HOSTNAME:
            self.read_token(next_byte, SPACE, APPNAME, HOSTNAME_TOKEN)
        elif self.current_state == APPNAME:
            self.read_token(next_byte, SPACE, PROCESSID, APPNAME_TOKEN)
        elif self.current_state == PROCESSID:
            self.read_token(next_byte, SPACE, MESSAGEID, PROCESSID_TOKEN)
        elif self.current_state == MESSAGEID:
            self.read_token(next_byte, SPACE, STRUCTURED_DATA_BEGIN, MESSAGEID_TOKEN)
        elif self.current_state == STRUCTURED_DATA_BEGIN:
            self.read_structured_data(next_byte)
        elif self.current_state == SD_ELEMENT_NAME:
            self.read_token(next_byte, SPACE, SD_FIELD_NAME, SDE_NAME_TOKEN)
        elif self.current_state == SD_FIELD_NAME:
            self.read_token(next_byte, EQUALS, SD_VALUE_BEGIN, SDE_FIELD_NAME_TOKEN)
        elif self.current_state == SD_VALUE_BEGIN:
            self.read_sd_value_start(next_byte)
        elif self.current_state == SD_VALUE_CONTENT:
            self.read_token(next_byte, QUOTE, SD_VALUE_END, SDE_FIELD_VALUE_TOKEN)
        elif self.current_state == SD_VALUE_END:
            self.read_sd_value_end(next_byte)
        elif self.current_state == STRUCTURED_DATA_END:
            self.read_sd_end(next_byte)
        elif self.current_state == MESSAGE:
            self.read_message(next_byte)
        self.octets_left -= 1

    cdef void read_octet(self, char next_byte):
        if next_byte == SPACE:
            self.parse_octet()
            self.current_state = PRIORITY_BEGIN
        else:
            self.collect(next_byte)

    cdef void parse_octet(self):
        cdef char *octet_buffer = <char*> malloc(sizeof(char) * self.buffered_octets)
        cdef int octets_read = self.buffered_octets + 1

        try:
            self._copy_into(octet_buffer)
            self.octets_left = atoi(octet_buffer) - octets_read
        finally:
            free(octet_buffer)

    cdef void read_priority_start(self, char next_byte):
        if next_byte != OPEN_ANGLE_BRACKET:
            raise Exception('Expected <')
        self.current_state = PRIORITY

    cdef void read_token(self, char next_byte, char terminator, lexer_state next_state, int token_type):
        if next_byte == terminator:
            self.buffer_token(token_type)
            self.current_state = next_state
        else:
            self.collect(next_byte)

    cdef void read_structured_data(self, char next_byte):
        if next_byte == OPEN_BRACKET:
            self.current_state = SD_ELEMENT_NAME

    cdef void read_sd_value_start(self, char next_byte):
        if next_byte == QUOTE:
            self.current_state = SD_VALUE_CONTENT

    cdef void read_sd_value_end(self, char next_byte):
        if next_byte != SPACE:
            if next_byte == CLOSE_BRACKET:
                self.current_state = STRUCTURED_DATA_END
            else:
                self.collect(next_byte)
                self.current_state = SD_FIELD_NAME

    cdef void read_sd_end(self, char next_byte):
        if next_byte != SPACE:
            if next_byte == OPEN_BRACKET:
                self.current_state = SD_ELEMENT_NAME
            else:
                self.read_message(next_byte)
                self.current_state = MESSAGE

    cdef void read_message(self, char next_byte):
        cdef bool done_reading_message = (self.octets_left - 1) == 0
        self.collect(next_byte)
        if done_reading_message or (self.buffered_octets + 1) == self.buffer_size:
            self.buffer_token(MESSAGE_PART_TOKEN)
        if done_reading_message:
            self.current_state = OCTET

