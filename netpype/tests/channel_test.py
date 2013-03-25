import unittest
import time

import netpype.channel as channel

class WhenManipulatingCyclicBuffers(unittest.TestCase):

    def test_init_with_buffer(self):
        buff = channel.CyclicBuffer(size_hint=10, data=bytearray('test'))
        self.assertEqual(4, buff.available())
        self.assertEqual(6, buff.remaining())

    def test_recycling(self):
        buff = channel.CyclicBuffer(size_hint=10, data=bytearray('test'))
        dest = bytearray(10)
        read = buff.get(dest)
        self.assertEqual(4, read)
        buff.put(bytearray('testing!!'))
        read = buff.get(dest)
        self.assertEqual(9, read)
        self.assertEqual(0, buff.available())
        buff.put(bytearray('testing'))
        read = buff.get(dest)
        self.assertEqual(7, read)
        self.assertEqual(0, buff.available())
        
    def test_get(self):
        buff = channel.CyclicBuffer(data=bytearray('test'))
        self.assertEqual(4, buff.available())
        dest = bytearray(buff.available())
        buff.get(dest, 0, 0)
        self.assertEqual(4, buff.available())
        buff.get(dest, 0)
        self.assertEqual(4, len(dest))

    def test_put(self):
        buff = channel.CyclicBuffer(size_hint=10)
        buff.put(bytearray('test'), 0)
        self.assertEqual(4, buff.available())
        self.assertEqual(6, buff.remaining())

    def test_get_until(self):
        buff = channel.CyclicBuffer(size_hint=10, data=bytearray('test test!'))
        self.assertEqual(10, buff.available())
        data = bytearray(10)

        # When the delim is not found, we return -1
        read = buff.get_until('_', data)
        self.assertEqual(-1, read)
        
        read = buff.get_until(' ', data)
        self.assertEqual(4, read)
        self.assertEqual(6, buff.available())
        self.assertEqual('t', chr(data[0]))
        self.assertEqual('e', chr(data[1]))
        self.assertEqual('s', chr(data[2]))
        self.assertEqual('t', chr(data[3]))
        buff.skip(1)
        read = buff.get_until('!', data, 4)
        self.assertEqual(1, buff.available())
        self.assertEqual('t', chr(data[4]))
        self.assertEqual('e', chr(data[5]))
        self.assertEqual('s', chr(data[6]))
        self.assertEqual('t', chr(data[7]))
        buff.skip(1)
        self.assertEqual(0, buff.available())

    def test_get_until_over_limit(self):
        buff = channel.CyclicBuffer(size_hint=10, data=bytearray('test test'))
        self.assertEqual(9, buff.available())
        data = bytearray(10)

        self.assertRaises(Exception, buff.get_until, ('_', data, 0, 1))

    def test_growing(self):
        buff = channel.CyclicBuffer(size_hint=10)
        buff.put(bytearray('More than you can handle.'), 0, 25)
        self.assertEqual(25, buff.available())


class WhenManipulatingChannelBuffers(unittest.TestCase):

    def test_init_with_buffer(self):
        channel_buffer = channel.ChannelBuffer(bytearray('bytes'))
        self.assertEqual(5, channel_buffer.size())

    def test_reading_buffer(self):
        channel_buffer = channel.ChannelBuffer(bytearray('bytes'))
        self.assertEqual(5, len(channel_buffer.remaining()))
        channel_buffer.sent(2)
        self.assertEqual(3, len(channel_buffer.remaining()))
        channel_buffer.sent(3)
        self.assertEqual(0, len(channel_buffer.remaining()))

    def test_checking_if_empty(self):
        channel_buffer = channel.ChannelBuffer(bytearray('bytes'))
        self.assertFalse(channel_buffer.empty())
        channel_buffer.sent(5)
        self.assertTrue(channel_buffer.empty())


if __name__ == '__main__':
    unittest.main()
