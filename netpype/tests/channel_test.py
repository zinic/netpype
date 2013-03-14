import time
import unittest
import logging
import multiprocessing

import netpype.channel as channel

class WhenManipulatingChannelBuffers(unittest.TestCase):

    def test_init_with_buffer(self):
        channel_buffer = channel.ChannelBuffer(b'bytes')
        self.assertEqual(5, channel_buffer.size())

    def test_reading_buffer(self):
        channel_buffer = channel.ChannelBuffer(b'bytes')
        self.assertEqual(5, len(channel_buffer.remaining()))
        channel_buffer.sent(2)
        self.assertEqual(3, len(channel_buffer.remaining()))
        channel_buffer.sent(3)
        self.assertFalse(channel_buffer.has_data())


if __name__ == '__main__':
    unittest.main()
