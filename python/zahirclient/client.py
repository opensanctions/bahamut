import grpc
from urllib.parse import urlparse

from zahirclient.proto import session_pb2_grpc, writer_pb2_grpc


class ZahirClient(object):
    def __init__(self, url):
        self.url = url
        parsed = urlparse(self.url)
        self.host = parsed.hostname
        self.port = parsed.port or "6674"
        self.channel = grpc.insecure_channel(f"{self.host}:{self.port}")
        self.session_service = session_pb2_grpc.SessionServiceStub(self.channel)
        self.writer_service = writer_pb2_grpc.WriterServiceStub(self.channel)
