from abc import ABC

from triplestore.triple import Triple
from triplestore.query import Query


class Store(ABC):
    def insert(self, triple: Triple):
        raise NotImplementedError

    def query(self, query: Query):
        raise NotImplementedError

    def delete(self, query: Query):
        raise NotImplementedError
