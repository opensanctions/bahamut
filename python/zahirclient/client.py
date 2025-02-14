from typing import Dict, Iterable, List
import logging
import grpc
from urllib.parse import urlparse

from nomenklatura.statement import Statement

from zahirclient.proto import view_pb2_grpc
from zahirclient.proto.view_pb2 import GetDatasetsRequest, GetDatasetVersionsRequest
from zahirclient.proto import writer_pb2_grpc
from zahirclient.proto.writer_pb2 import (
    WriteStatement,
    ReleaseDatasetRequest,
    DeleteDatasetRequest,
)
from zahirclient.util import datetime_ts

log = logging.getLogger(__name__)


class ZahirClient(object):
    def __init__(self, url):
        self.url = url
        parsed = urlparse(self.url)
        self.host = parsed.hostname
        self.port = parsed.port or "6674"
        self.channel = grpc.insecure_channel(f"{self.host}:{self.port}")
        self.view_service = view_pb2_grpc.ViewServiceStub(self.channel)
        self.writer_service = writer_pb2_grpc.WriterServiceStub(self.channel)

    def get_datasets(self) -> Dict[str, str]:
        resp = self.view_service.GetDatasets(GetDatasetsRequest())
        versions: Dict[str, str] = {}
        for dataset in resp.datasets:
            versions[dataset.name] = dataset.version
        return versions

    def get_dataset_versions(self, dataset: str) -> List[str]:
        resp = self.view_service.GetDatasetVersions(
            GetDatasetVersionsRequest(dataset=dataset)
        )
        return resp.versions

    def write_statements(self, version: str, statements: Iterable[Statement]):
        def generate():
            try:
                for stmt in statements:
                    yield WriteStatement(
                        id=stmt.id,
                        entity_id=stmt.entity_id,
                        schema=stmt.schema,
                        property=stmt.prop,
                        dataset=stmt.dataset,
                        value=stmt.value,
                        lang=stmt.lang,
                        originalValue=stmt.original_value,
                        external=stmt.external,
                        first_seen=datetime_ts(stmt.first_seen),
                        last_seen=datetime_ts(stmt.last_seen),
                        version=version,
                    )
            except Exception:
                log.exception("Error while writing statements!")

        self.writer_service.WriteDataset(generate())

    def release_dataset(self, dataset: str, version: str) -> None:
        self.writer_service.ReleaseDataset(
            ReleaseDatasetRequest(dataset=dataset, version=version)
        )

    def delete_dataset_version(self, dataset: str, version: str) -> bool:
        resp = self.writer_service.DeleteDatasetVersion(
            DeleteDatasetRequest(dataset=dataset, version=version)
        )
        return resp.success
