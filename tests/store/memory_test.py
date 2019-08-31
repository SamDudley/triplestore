import pytest

from triplestore.store.memory import MemoryStore
from triplestore.triple import Triple
from triplestore.query import Query, Clause, Type


@pytest.fixture
def store():
    return MemoryStore()


def test_insert(store):
    triple = Triple('Sam', 'loves', 'Sam', 1.0)
    store.insert(triple)

    assert len(store.memory) == 1


def test_duplicate_insert(store):
    triple = Triple('Sam', 'loves', 'Sam', 1.0)
    store.insert(triple)
    store.insert(triple)

    assert len(store.memory) == 1


def test_any_query(store):
    triple = Triple('Sam', 'loves', 'Sam', 1.0)
    store.insert(triple)

    all_query = Query()

    results = list(store.query(all_query))

    assert len(results) == 1


def test_one_query(store):
    store.insert(Triple('Sam', 'loves', 'Sam', 1.0))
    store.insert(Triple('Sam', 'owns', 'Car', 1.0))

    query = Query(predicate=Clause(type=Type.EQ, value='loves'))

    results = list(store.query(query))

    assert len(results) == 1


def test_delete(store):
    store.insert(Triple('Sam', 'loves', 'Sam', 1.0))
    store.insert(Triple('Sam', 'owns', 'Car', 1.0))

    query = Query(predicate=Clause(type=Type.EQ, value='owns'))

    results = store.delete(query)

    assert results == 1
    assert len(store.memory) == 1
