from typing import List
import grpc
from urllib.parse import urlparse

from nomenklatura.statement import Statement

from zahirclient.proto import view_pb2_grpc
from zahirclient.proto import writer_pb2_grpc
from zahirclient.proto.writer_pb2 import WriteStatement, WriteDatasetResponse


class ZahirClient(object):
    def __init__(self, url):
        self.url = url
        parsed = urlparse(self.url)
        self.host = parsed.hostname
        self.port = parsed.port or "6674"
        self.channel = grpc.insecure_channel(f"{self.host}:{self.port}")
        self.view_service = view_pb2_grpc.ViewServiceStub(self.channel)
        self.writer_service = writer_pb2_grpc.WriterServiceStub(self.channel)

    def write_statements(self, dataset: str, version: str, statements: List[Statement]):
        def generate():
            for stmt in statements:
                yield WriteStatement(
                    id=stmt.id,
                    dataset=dataset,
                    version=version,
                )
                yield statement.to_pb()

        self.writer_service.WriteDataset(generate())
