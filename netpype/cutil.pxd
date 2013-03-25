cdef class CyclicBuffer(object):

    cdef char *_buffer
    cdef int _current_size, _read_index, _write_index, _available
    
    cdef int _get(self, char* data, int offset, int length)
    cdef void _put(self, char *data, int offset, int length)
    cpdef int skip(self, int length)
    cpdef grow(self, int min_length)
    cpdef int available(self)
    cpdef int remaining(self)
    cpdef clear(self)
