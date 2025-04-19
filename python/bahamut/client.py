from typing import Dict, Generic, Iterable, List, Type
import logging
import grpc
from urllib.parse import urlparse

from nomenklatura.dataset import DS
from nomenklatura.entity import CE
from nomenklatura.statement import Statement

from bahamut.proto import view_pb2_grpc
from bahamut.proto.view_pb2 import ViewEntity
from bahamut.proto.view_pb2 import GetDatasetsRequest
from bahamut.proto.view_pb2 import GetDatasetVersionsRequest
from bahamut.proto import writer_pb2_grpc
from bahamut.proto.writer_pb2 import (
    WriteStatement,
    ReleaseDatasetRequest,
    DeleteDatasetRequest,
)
from bahamut.util import datetime_ts, ts_iso

log = logging.getLogger(__name__)


class BahamutClient(Generic[DS, CE]):
    def __init__(self, entity_class: Type[CE], dataset: DS, url: str) -> None:
        self.entity_class = entity_class
        self.dataset = dataset
        self.url = url
        parsed = urlparse(self.url)
        self.host = parsed.hostname
        self.port = parsed.port or "6674"
        self.channel = grpc.insecure_channel(f"{self.host}:{self.port}")
        self.view_service = view_pb2_grpc.ViewServiceStub(self.channel)
        self.writer_service = writer_pb2_grpc.WriterServiceStub(self.channel)

    def _convert_entity(self, ve: ViewEntity) -> CE:
        statements: List[Statement] = []
        for vs in ve.statements:
            stmt = Statement(
                id=vs.id,
                entity_id=vs.entity_id,
                canonical_id=ve.id,
                schema=vs.schema,
                prop=vs.property,
                dataset=vs.dataset,
                value=vs.value,
                lang=vs.lang,
                original_value=vs.originalValue,
                external=vs.external,
                first_seen=ts_iso(vs.first_seen),
                last_seen=ts_iso(vs.last_seen),
            )
            statements.append(stmt)

        entity = self.entity_class.from_statements(self.dataset, statements)
        entity._caption = ve.caption
        entity.extra_referents.update(ve.referents)
        return entity

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
