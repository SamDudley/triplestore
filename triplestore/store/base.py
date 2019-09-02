from typing import Iterable

from abc import ABC

from triplestore.triple import Triple
from triplestore.query import Query


class Store(ABC):
    def insert(self, triple: Triple) -> None:
        raise NotImplementedError

    def query(self, query: Query) -> Iterable[Triple]:
        raise NotImplementedError

    def delete(self, query: Query) -> int:
        raise NotImplementedError

    def count(self) -> int:
        raise NotImplementedError
