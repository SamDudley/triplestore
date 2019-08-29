import pytest

from triplestore.store.memory import MemoryStore
from triplestore.triple import Triple
from triplestore.query import Query


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


def test_query(store):
    triple = Triple('Sam', 'loves', 'Sam', 1.0)
    store.insert(triple)

    all_query = Query()

    results = list(store.query(all_query))

    assert len(results) == 1
