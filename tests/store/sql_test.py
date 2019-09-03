from typing import Set

import sqlite3

import pytest

from triplestore.store.sql import SqlStore
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
    yield conn
    conn.close()


@pytest.fixture
def store(conn):
    conn.execute('BEGIN')

    store = SqlStore(conn)
    store.setup()

    for triple in TEST_TRIPLES:
        store.insert(triple)

    yield store

    conn.execute('ROLLBACK')


def test_count(store):
    assert store.count() == len(TEST_TRIPLES)


def test_insert(store):
    store.insert(Triple('Jack', 'knows', 'Matt', 1.0))

    assert store.count() == (len(TEST_TRIPLES) + 1)
