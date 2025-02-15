from zavod.logs import get_logger
from zavod.meta import get_catalog
from zavod.runtime.versions import get_latest
from zavod.archive import iter_dataset_statements

from zahirclient.client import ZahirClient

log = get_logger("zavod_sync")

catalog = get_catalog()
scope = catalog.require("sanctions")
client = ZahirClient("http://localhost:6674")
server_versions = client.get_datasets()
for dataset in scope.datasets:
    if dataset.is_collection:
        continue
    ds_version = get_latest(dataset.name)
    if ds_version is None:
        continue
    latest_version = str(ds_version)
    server_version = server_versions.get(dataset.name, None)
    if latest_version == server_version:
        log.info("Dataset is up to date", dataset=dataset.name, version=latest_version)
        continue
    log.info(
        "Syncing dataset...",
        dataset=dataset.name,
        latest_version=latest_version,
        server_version=server_version,
    )
    client.write_statements(latest_version, iter_dataset_statements(dataset))
    client.release_dataset(dataset.name, latest_version)


# # view = store.default_view()
# for idx, ent in enumerate(view.entities()):
#     if idx > 0 and idx % 10_000 == 0:
#         log.info("Loading entities...", entities=idx, scope=scope.name)
#         for i in range(5):
#             ent.to_nested_dict(view)
