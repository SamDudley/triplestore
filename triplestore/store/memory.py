from typing import Set

from .base import Store

from triplestore.triple import Triple
from triplestore.query import Query


class MemoryStore(Store):
    def __init__(self):
        self.memory: Set[Triple] = set()

    def insert(self, triple: Triple):
        self.memory.add(triple)

    def query(self, query: Query):
        # how many results we have returned so far
        count = 0

        for triple in self.memory:
            if query.limit > 0 and count >= query.limit:
                continue

            # TODO: implement matching logic

            yield triple

    def delete(self, query: Query):
        raise NotImplementedError
