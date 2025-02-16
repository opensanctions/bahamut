from typing import TYPE_CHECKING, Dict, Generator, Optional
from nomenklatura.store.base import View
from nomenklatura.entity import CE
from nomenklatura.dataset import DS

from zahirclient.proto.view_pb2 import DatasetSpec
from zahirclient.proto.view_pb2 import CreateViewRequest
from zahirclient.proto.view_pb2 import CloseViewRequest
from zahirclient.proto.view_pb2 import EntityStreamRequest
from zahirclient.proto.view_pb2 import EntityRequest

if TYPE_CHECKING:
    from zahirclient.client import ZahirClient


class ZahirView(View[DS, CE]):
    def __init__(
        self,
        client: "ZahirClient[DS, CE]",
        scope: Dict[str, str],
        unresolved: bool = False,
        external: bool = False,
    ) -> None:
        self.client = client
        self.scope = scope
        self.unresolved = unresolved
        self.external = external

        self.view_id = client.view_service.CreateView(
            CreateViewRequest(
                scope=[DatasetSpec(name=k, version=v) for k, v in scope.items()],
                unresolved=unresolved,
                withExternal=external,
            )
        ).view_id

    def get_entity(self, id: str) -> Optional[CE]:
        req = EntityRequest(view_id=self.view_id, entity_id=id)
        resp = self.client.view_service.GetEntity(req)
        if resp.entity is None:
            return None
        return self.client._convert_entity(resp.entity)

    def entities(self) -> Generator[CE, None, None]:
        req = EntityStreamRequest(view_id=self.view_id)
        for ve in self.client.view_service.GetEntities(req):
            yield self.client._convert_entity(ve)

    def close(self) -> None:
        self.client.view_service.CloseView(CloseViewRequest(view_id=self.view_id))
