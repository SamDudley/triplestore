from typing import Set

import sqlite3

import pytest

from triplestore.exceptions import DuplicateError
from triplestore.store.memory import MemoryStore
from triplestore.store.sqlite import SqliteStore
from triplestore.triple import Triple
from triplestore.query import Query, Clause, Type


TEST_TRIPLES: Set[Triple] = set([
    Triple('Sam', 'knows', 'Jack', 1.0),
    Triple('Sam', 'knows', 'Matt', 1.0),
    Triple('Sam', 'knows', 'Dan', 1.0),
    Triple('Sam', 'works_with', 'Jack', 1.0),
    Triple('Sam', 'works_with', 'Dan', 1.0),
])


@pytest.fixture(scope='module')
def conn():
    conn = sqlite3.connect(':memory:')

    SqliteStore.setup(conn)

    yield conn

    conn.close()


def pytest_generate_tests(metafunc):
    if 'store' in metafunc.fixturenames:
        metafunc.parametrize(
            argnames='store',
            argvalues=[MemoryStore, SqliteStore],
            ids=['memory', 'sql'],
            indirect=True,
            scope='function'
        )


@pytest.fixture
def store(request, conn):
    if request.param is MemoryStore:
        yield MemoryStore()
    elif request.param is SqliteStore:
        conn.execute('BEGIN')

        store = SqliteStore(conn)

        yield store

        conn.execute('ROLLBACK')
    else:
        raise ValueError('Invalid internal test config')


def test_count(store):
    assert store.count() == 0


def test_insert(store):
    store.insert(Triple('Jack', 'knows', 'Matt', 1.0))

    assert store.count() == 1


def test_duplicate_insert(store):
    triple = Triple('Jack', 'knows', 'Dan', 1.0)
    store.insert(triple)

    with pytest.raises(DuplicateError):
        store.insert(triple)

    assert store.count() == 1


def test_any_query(store):
    for triple in TEST_TRIPLES:
        store.insert(triple)

    all_query = Query()

    results = list(store.query(all_query))

    assert len(results) == len(TEST_TRIPLES)


def test_eq_query(store):
    for triple in TEST_TRIPLES:
        store.insert(triple)

    query = Query(predicate=Clause(type=Type.EQ, value='knows'))

    results = list(store.query(query))

    assert len(results) == 3


def test_delete(store):
    for triple in TEST_TRIPLES:
        store.insert(triple)

    query = Query(predicate=Clause(type=Type.EQ, value='works_with'))

    results = store.delete(query)

    assert results == 2
    assert store.count() == 3
