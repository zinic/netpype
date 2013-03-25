cdef enum _sd_token:
    PRIORITY_TOKEN,
    VERSION_TOKEN,
    TIMESTAMP_TOKEN,
    HOSTNAME_TOKEN,
    APPNAME_TOKEN,
    PROCESSID_TOKEN,
    MESSAGEID_TOKEN,
    SDE_NAME_TOKEN,
    SDE_FIELD_NAME_TOKEN,
    SDE_FIELD_VALUE_TOKEN,
    MESSAGE_PART_TOKEN

ctypedef _sd_token sd_token

cdef enum _state:
    OCTET
    PRIORITY_BEGIN
    PRIORITY
    VERSION
    TIMESTAMP
    HOSTNAME
    APPNAME
    PROCESSID
    MESSAGEID
    STRUCTURED_DATA_BEGIN
    SD_ELEMENT_NAME
    SD_FIELD_NAME
    SD_VALUE_BEGIN
    SD_VALUE_CONTENT
    SD_VALUE_END
    STRUCTURED_DATA_END
    MESSAGE

ctypedef _state state

# Delimeter constants
cdef char SPACE = ' '
cdef char QUOTE = '"'
cdef char EQUALS = '='
cdef char CLOSE_ANGLE_BRACKET = '>'
cdef char OPEN_BRACKET = '['
cdef char CLOSE_BRACKET = ']'

cdef int RFC3164_MAX_BYTES = 1024
cdef int RFC5424_MAX_BYTES = 2048

cdef int MAX_BYTES = 536870912
