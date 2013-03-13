import socket
import select
import logging


_LOG = logging.getLogger('netpype.channel')

UNIX_SOCK = socket.AF_UNIX
IPv4_SOCK = socket.AF_INET
IPv6_SOCK = socket.AF_INET6


def server_socket(socket_inet_addr):
    ssock = socket.socket(socket_inet_addr.type, socket.SOCK_STREAM)
    ssock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ssock.bind((socket_inet_addr.address, socket_inet_addr.port))
    ssock.setblocking(0)
    ssock.listen(1)
    return ssock


class SocketINetAddress(object):

    def __init__(self, type, address, port):
        self.type = type
        self.address = address
        self.port = port

    def __repr__(self):
        return '{} socket {}:{}'.format(self.type,
                                        self.address,
                                        self.port)


class PipelineFactory(object):

    def new_upstream_pipeline(self):
        raise NotImplementedError

    def new_downstream_pipeline(self):
        raise NotImplementedError


class ChannelPipeline(object):

    def __init__(self, pipeline_factory):
        self.upstream = pipeline_factory.new_upstream_pipeline()
        self.downstream = pipeline_factory.new_upstream_pipeline()


class ChannelHandler(object):

    def __init__(self, channel, pipeline):
        self.channel = channel
        self.pipeline = pipeline
        self.write_buffer = b''


class AbstractEPollHandler(object):

    def on_connect(self, address):
        return REQUEST_CLOSE, None

    def on_close(self, address):
        return None

    def on_read(self, channel):
        return REQUEST_CLOSE, None

    def on_write(self, channel):
        return REQUEST_CLOSE, None
