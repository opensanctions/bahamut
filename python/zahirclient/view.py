from typing import TYPE_CHECKING, Generator
from nomenklatura.store.base import View
from nomenklatura.entity import CE

if TYPE_CHECKING:
    from zahirclient.client import ZahirClient


class ZahirView(View):
    def __init__(self, client: "ZahirClient") -> None:
        self.client = client

    def entities(self) -> Generator[CE, None, None]:
        raise NotImplementedError
