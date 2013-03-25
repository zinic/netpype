from libc.stdlib cimport malloc, realloc, free
from cython import array


cdef extern from "Python.h":
    char* PyByteArray_AS_STRING(object bytearray) except NULL
    int PyByteArray_Check(object bytearray)
    int PyByteArray_GET_SIZE(object bytearray)

    
def buffer_seek(char delim, object source, int size, int read_index, int available):
    return c_buffer_seek(delim, PyByteArray_AS_STRING(source), size, read_index, available)

    
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


cdef direct_copy(char *source, int soffset, char *dest, int doffset, int length):
    cdef int ioffset = 0
    while ioffset < length:
        dest[doffset + ioffset] = source[soffset + ioffset]
        ioffset += 1

cdef array_copy(char *source, int soffset, char[:] dest, int doffset, int length):
    cdef int ioffset = 0
    while ioffset < length:
        dest[doffset + ioffset] = source[soffset + ioffset]
        ioffset += 1

        
cdef class CyclicBuffer(object):
    
    def __cinit__(self, int size_hint=4096):
        self._buffer = <char*> malloc(sizeof(char) * size_hint)
        self._current_size = size_hint
        self.clear()

    def __dealloc__(self):
        free(self._buffer)
        
    def skip_until(self, char *delims, int limit=-1):
        cdef int seek_offset
        seek_offset = c_buffer_seek(delims[0], self._buffer, 
            self._current_size, self._read_index, self._available)
        if seek_offset > 0:
            return self.skip(seek_offset)
        return seek_offset

    def get_until(self, char *delims, char[:] data, int offset=0, int limit=-1):
        cdef int seek_offset
        seek_offset = c_buffer_seek(delims[0], self._buffer, 
            self._current_size, self._read_index, self._available)
        if seek_offset > 0:
            return self.get(data, offset, seek_offset)
        return seek_offset

    cdef int _get(self, char* data, int offset, int length):
        cdef int trimmed_length, next_read_index
        cdef int readable = 0
        if self._available > 0:
            if length > self._available:
                readable = self._available
            else:
                readable = length
            
            if self._read_index + readable >= self._current_size:
                trimmed_length = self._current_size - self._read_index
                next_read_index = readable - trimmed_length
                direct_copy(self._buffer, self._read_index, data,
                           offset, trimmed_length)
                direct_copy(self._buffer, 0, data,
                           offset + trimmed_length, next_read_index)
                self._read_index = next_read_index
            else:
                direct_copy(self._buffer, self._read_index,
                           data, offset, readable)
                if self._read_index + readable < self._current_size:
                    self._read_index += readable
                else:
                    self._read_index = readable - (
                        self._current_size - self._read_index)
            self._available -= readable
        return readable
        
    def get(self, char[:] data, int offset=0, int length=-1):
        cdef int trimmed_length, next_read_index
        cdef int readable = 0
        if self._available > 0:
            if length == -1 or length > self._available:
                readable = self._available
            else:
                readable = length
            
            if self._read_index + readable >= self._current_size:
                trimmed_length = self._current_size - self._read_index
                next_read_index = readable - trimmed_length
                array_copy(self._buffer, self._read_index, data,
                           offset, trimmed_length)
                array_copy(self._buffer, 0, data,
                           offset + trimmed_length, next_read_index)
                self._read_index = next_read_index
            else:
                array_copy(self._buffer, self._read_index,
                           data, offset, readable)
                if self._read_index + readable < self._current_size:
                    self._read_index += readable
                else:
                    self._read_index = readable - (
                        self._current_size - self._read_index)
            self._available -= readable
        return readable

    cdef void _put(self, char *data, int offset, int length):
        cdef int remaining, trimmed_length, next_write_index
        remaining = self._current_size - self._available
        if remaining < length:
            self.grow(length - remaining)
        if self._write_index + length >= self._current_size:
            trimmed_length = self._current_size - self._write_index
            next_write_index = length - trimmed_length
            direct_copy(data, offset, self._buffer,
                        self._write_index, trimmed_length)
            direct_copy(data, offset + trimmed_length,
                        self._buffer, 0, next_write_index)
            self._write_index = next_write_index
        else:
            direct_copy(data, offset, self._buffer,
                        self._write_index, length)
            self._write_index += length
        self._available += length
        
    def put(self, object data, int offset=0, int length=-1):
        if not PyByteArray_Check(data):
            raise Exception
        cdef int size = length
        if length == -1:
            size = PyByteArray_GET_SIZE(data)
        self._put(PyByteArray_AS_STRING(data),
                  offset,
                  size)

    cpdef int skip(self, int length):
        cdef int bytes_skipped = 0
        if self._available > 0:
            if length > self._available:
                bytes_skipped = self._available
                self._read_index = 0
                self._write_index = 0
            else:
                bytes_skipped = length
                if self._read_index + length < self._current_size:
                    self._read_index += length
                else:
                    self._read_index = length - self._current_size
                    self._read_index -= self._read_index
            self._available -= bytes_skipped
        return bytes_skipped

    cpdef grow(self, int min_length):
        cdef int new_size = self._current_size * 2 * (
            int(min_length / self._current_size) + 1)
        cdef char* new_buffer = <char*> malloc(sizeof(char) * new_size)
        cdef int read = self._get(new_buffer, 0, new_size)
        free(self._buffer)
        self._buffer = new_buffer
        self._current_size = new_size
        self._read_index = 0
        self._write_index = read

    cpdef int available(self):
        return self._available

    cpdef int remaining(self):
        return self._current_size - self._available

    cpdef clear(self):
        self._read_index = 0
        self._write_index = 0
        self._available = 0
