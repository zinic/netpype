from cpython cimport array

cdef extern from "Python.h":
    char* PyByteArray_AsString(object bytearray) except NULL
    
def buffer_seek(char delim, object source, int size, int read_index, int available):
    return c_buffer_seek(delim, PyByteArray_AsString(source), size, read_index, available)

cdef c_buffer_seek(char delim, char *data, int size, int read_index, int available):
    cdef int seek_offset = 0
    cdef int seek_index = read_index
    while seek_offset <= available:
        if data[seek_index] == delim:
            return seek_offset

        if seek_index + 1 >= size:
            seek_index = 0
        else:
            seek_index += 1
        seek_offset += 1
    return -1
