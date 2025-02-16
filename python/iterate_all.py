from zavod.logs import get_logger
from zavod.meta import get_catalog
from zavod.entity import Entity

from zahirclient.client import ZahirClient
from zahirclient.proto.view_pb2 import (
    EntityStreamRequest,
    CloseViewRequest,
    CreateViewRequest,
)

log = get_logger("iterate_all")
catalog = get_catalog()
dataset = catalog.make_dataset({"name": "test", "title": "test"})
client = ZahirClient(Entity, dataset, "http://localhost:6674")
server_versions = client.get_datasets()

resp = client.view_service.CreateView(CreateViewRequest(unresolved=False))
view_id = resp.view_id

statements = 0
for idx, viewentity in enumerate(
    client.view_service.GetEntities(EntityStreamRequest(view_id=view_id))
):
    if idx > 0 and idx % 10_000 == 0:
        log.info("Loading entities...", entities=idx, statements=statements)
    # viewentity.id
    client._convert_entity(viewentity)
    statements += len(viewentity.statements)

client.view_service.CloseView(CloseViewRequest(view_id=view_id))
