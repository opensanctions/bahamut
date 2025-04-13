from typing import List
from pathlib import Path
from nomenklatura.dataset import Dataset
from nomenklatura.entity import CompositeEntity
from nomenklatura.statement.serialize import get_statement_writer, CSV

RESOURCE_PATH = Path(__file__).parent.parent / "src" / "test" / "resources" / "fixtures"
RESOURCE_PATH.mkdir(parents=True, exist_ok=True)


def make_entity(dataset: str, schema: str, entity_id: str) -> CompositeEntity:
    """Create a CompositeEntity with the given schema, entity_id, and dataset."""
    dataset_ = Dataset.make({"name": dataset, "title": dataset})
    return CompositeEntity(dataset_, {"schema": schema, "id": entity_id})


def dump_statements(dataset: str, entities: List[CompositeEntity]) -> None:
    """Dump the statements of the given entities to a CSV file."""
    with open(RESOURCE_PATH / f"{dataset}.csv", "wb") as fh:
        writer = get_statement_writer(fh, format=CSV)
        for entity in entities:
            for statement in entity.statements:
                writer.write(statement)
        writer.close()


def make_fixtures_dataset1():
    entities = []
    dataset1 = "test_dataset1"
    entity1 = make_entity(dataset1, "Person", "td1-john-gruber")
    entity1.set("name", "John Gruber", lang="eng")
    entity1.set("birthDate", "1942")
    entity1.set("nationality", "DD")
    entities.append(entity1)
    entity2 = make_entity(dataset1, "Person", "td1-jane-gruber")
    entity2.set("name", "Jasmin Gruber", lang="deu")
    entity2.set("birthDate", "1948")
    entity2.set("nationality", "PL")
    entities.append(entity2)
    link1 = make_entity(dataset1, "Family", "td1-john-gruber-jane-gruber")
    link1.set("person", "td1-john-gruber")
    link1.set("relative", "td1-jane-gruber")
    link1.set("relationship", "spouse")
    link1.set("startDate", "1970")
    entities.append(link1)
    entity5 = make_entity(dataset1, "Person", "Q844")
    entity5.set("name", "James Bond", lang="eng")
    entity5.set("birthDate", "1930")
    entity5.set("nationality", "GB-SCT")
    entity5.set("topics", "role.spy")
    entities.append(entity5)
    entity6 = make_entity(dataset1, "Person", "td1-goldfinger")
    entity6.set("name", "Auric Goldfinger")
    entity6.set("birthDate", "1920")
    entity6.set("topics", "crime.boss")
    entities.append(entity6)
    entity3 = make_entity(dataset1, "Organization", "td1-spectre")
    entity3.set("name", "SPECTRE")
    entity3.set(
        "alias", "Special Executive for Counter-Intelligence, Revenge and Extortion"
    )
    entity3.set("incorporationDate", "1999")
    entity3.set("jurisdiction", "Bahamas")
    entities.append(entity3)
    link2 = make_entity(dataset1, "Directorship", "td1-goldfinger-spectre")
    link2.set("director", "td1-goldfinger")
    link2.set("organization", "td1-specre")
    link2.set("startDate", "1999")
    link2.set("endDate", "2000")
    link2.set("role", "Chairman")
    entities.append(link2)
    entity4 = make_entity(dataset1, "Organization", "td1-umbrella")
    entity4.set("name", "Umbrella Corporation")
    entity4.set("incorporationDate", "1968")
    entity4.set("jurisdiction", "USA")
    entities.append(entity4)
    entity10 = make_entity(dataset1, "Person", "td1-john-gruber")
    entity10.set("name", "John Gruber", lang="eng")
    entity10.set("nationality", "DD")
    entity10.set("topics", "crime.terror")
    entities.append(entity10)
    dump_statements(dataset1, entities)


def make_fixtures_dataset2():
    dataset = "test_sanctions"
    entities = []
    entity1 = make_entity(dataset, "Person", "tsa-john-gruber")
    entity1.set("name", "John Gruber", lang="fra")
    entity1.set("birthDate", "1942-02-12")
    entity1.set("topics", "sanction")
    entities.append(entity1)
    entity2 = make_entity(dataset, "Person", "Q7747")
    entity2.set("name", "Vladimir Putin", lang="eng")
    entity2.set("name", "Vladimir Vladimirovitch Putin", lang="eng")
    entity2.set("name", "Vladimir V. Putin", lang="eng")
    entity2.set("name", "VLADIMIR PUTIN", lang="eng")
    entity2.set("name", "Владимир Путин", lang="rus")
    entity2.set("name", "Владимир Владимирович Путин", lang="rus")
    entity2.set("name", "ウラジーミル・プーチン", lang="jpn")
    entity2.set("birthDate", "1952-10-07")
    entity2.set("topics", "sanction")
    entities.append(entity2)
    entity3 = make_entity(dataset, "Person", "auric-gf")
    entity3.set("name", "Auric Goldfinger")
    entity3.set("birthDate", "1920-01-01")
    entity3.set("topics", "sanction")
    entities.append(entity3)
    dump_statements(dataset, entities)


if __name__ == "__main__":
    make_fixtures_dataset1()
    make_fixtures_dataset2()
    print(f"Fixtures created in {RESOURCE_PATH}")
