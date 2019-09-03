from typing import Iterable

from triplestore.triple import Triple
from triplestore.query import Query, Clause, Type

from .base import Store


CREATE_TABLE_SQL: str = """
    CREATE TABLE IF NOT EXISTS triple (
        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        source TEXT NOT NULL,
        predicate TEXT NOT NULL,
        target TEXT NOT NULL,
        weight DECIMAL NOT NULL
    );

    CREATE UNIQUE INDEX IF NOT EXISTS triple_index ON triple (source, predicate, target);
"""


class SqlStore(Store):
    def __init__(self, conn) -> None:
        self.conn = conn

    def insert(self, triple: Triple) -> None:
        query = """
            INSERT INTO triple (source, predicate, target, weight)
            VALUES (?, ?, ?, ?)
        """

        values = (
            triple.source, triple.predicate, triple.target, triple.weight
        )

        self.conn.execute(query, values)

    def query(self, query: Query) -> Iterable[Triple]:
        raise NotImplementedError

    def delete(self, query: Query) -> int:
        raise NotImplementedError

    def count(self) -> int:
        query = 'SELECT count(*) FROM triple'

        cur = self.conn.cursor()
        cur.execute(query)

        return cur.fetchone()[0]

    def setup(self) -> None:
        self.conn.executescript(CREATE_TABLE_SQL)
