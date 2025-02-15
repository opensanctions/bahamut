from zavod.logs import get_logger

from zahirclient.client import ZahirClient

from zahirclient.proto.view_pb2 import (
    EntityStreamRequest,
    CloseViewRequest,
    CreateViewRequest,
)

log = get_logger("iterate_all")

client = ZahirClient("http://localhost:6674")
server_versions = client.get_datasets()

resp = client.view_service.CreateView(CreateViewRequest())
view_id = resp.view_id

statements = 0
for idx, viewentity in enumerate(
    client.view_service.GetEntities(EntityStreamRequest(view_id=view_id))
):
    if idx > 0 and idx % 10_000 == 0:
        log.info("Loading entities...", entities=idx, statements=statements)
    # viewentity.id
    statements += len(list(viewentity.statements))
    for stmt in viewentity.statements:
        # stmt.subject
        # stmt.predicate
        # stmt.object
        print(stmt.id, stmt.property, stmt.value)

client.view_service.CloseView(CloseViewRequest(view_id=view_id))
