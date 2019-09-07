from typing import Set

import pytest

from triplestore.store.memory import MemoryStore
from triplestore.triple import Triple
from triplestore.query import Query, Clause, Type


TEST_TRIPLES: Set[Triple] = set([
    Triple('Sam', 'knows', 'Jack', 1.0),
    Triple('Sam', 'knows', 'Matt', 1.0),
    Triple('Sam', 'knows', 'Dan', 1.0),
    Triple('Sam', 'works_with', 'Jack', 1.0),
    Triple('Sam', 'works_with', 'Dan', 1.0),
])


@pytest.fixture
def store():
    store = MemoryStore()

    for triple in TEST_TRIPLES:
        store.insert(triple)

    return store


def test_count(store):
    assert store.count() == len(TEST_TRIPLES)


def test_insert(store):
    store.insert(Triple('Jack', 'knows', 'Matt', 1.0))

    assert store.count() == (len(TEST_TRIPLES) + 1)


def test_duplicate_insert(store):
    triple = Triple('Jack', 'knows', 'Dan', 1.0)
    store.insert(triple)
    store.insert(triple)

    assert store.count() == (len(TEST_TRIPLES) + 1)


def test_any_query(store):
    all_query = Query()

    results = list(store.query(all_query))

    assert len(results) == len(TEST_TRIPLES)


def test_eq_query(store):
    query = Query(predicate=Clause(type=Type.EQ, value='knows'))

    results = list(store.query(query))

    assert len(results) == 3


def test_delete(store):
    query = Query(predicate=Clause(type=Type.EQ, value='works_with'))

    results = store.delete(query)

    assert results == 2
    assert store.count() == 3
