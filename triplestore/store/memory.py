from typing import Set, Iterable, List

from .base import Store

from triplestore.triple import Triple
from triplestore.query import Query, Clause, Type


class MemoryStore(Store):
    def __init__(self):
        self.memory: Set[Triple] = set()

    def insert(self, triple: Triple):
        # TODO: should we throw an error if we try to insert a dupe?
        self.memory.add(triple)

    def query(self, query: Query) -> Iterable[Triple]:
        # how many results we have returned so far
        count = 0

        for triple in self.memory:
            if query.limit > 0 and count >= query.limit:
                continue

            if query.is_any():
                yield triple
                count += 1
            elif is_match(triple, query):
                yield triple
                count += 1

    def delete(self, query: Query) -> int:
        to_be_removed: List[Triple] = []

        for triple in self.memory:
            if query.limit > 0 and len(to_be_removed) >= query.limit:
                continue

            if query.is_any():
                to_be_removed.append(triple)
            elif is_match(triple, query):
                to_be_removed.append(triple)

        for triple in to_be_removed:
            self.memory.remove(triple)

        return len(to_be_removed)

    def count(self) -> int:
        return len(self.memory)


def is_match(triple: Triple, query: Query) -> bool:
    all_clauses_match = all([
        match_clause(triple.source, query.source),
        match_clause(triple.predicate, query.predicate),
        match_clause(triple.target, query.target),
    ])

    if not all_clauses_match:
        return False

    # FIXME: implement weight match

    return True


def match_clause(actual: str, clause: Clause) -> bool:
    if clause.is_any():
        return True

    if clause.type == Type.EQ:
        return actual == clause.value

    # FIXME: use a better exception
    raise Exception('Unsupported clause')
