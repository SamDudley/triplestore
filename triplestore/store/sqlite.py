from typing import Iterable, List, Tuple, Any

import sqlite3

from triplestore.exceptions import DuplicateError
from triplestore.triple import Triple
from triplestore.query import Query, Clause, Type

from .base import Store


CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS triple (
        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        source TEXT NOT NULL,
        predicate TEXT NOT NULL,
        target TEXT NOT NULL,
        weight DECIMAL NOT NULL
    );

    CREATE UNIQUE INDEX IF NOT EXISTS triple_index ON triple (source, predicate, target);
"""


class SqliteStore(Store):
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

        try:
            self.conn.execute(query, values)
        except sqlite3.IntegrityError:
            raise DuplicateError

    def query(self, query: Query) -> Iterable[Triple]:
        sql = 'SELECT * FROM triple'

        where, args = where_clause(query)
        if where:
            sql += f' WHERE {where}'

        if query.limit > 0:
            sql += ' LIMIT ?'
            args.append(query.limit)

        for row in self.conn.execute(sql, args):
            yield row

    def delete(self, query: Query) -> int:
        sql = 'DELETE FROM triple'

        where, args = where_clause(query)
        if where:
            sql += f' WHERE {where}'

        if query.limit > 0:
            sql += ' LIMIT ?'
            args.append(query.limit)

        return self.conn.execute(sql, args).rowcount

    def count(self) -> int:
        query = 'SELECT count(*) FROM triple'

        cur = self.conn.cursor()
        cur.execute(query)

        return cur.fetchone()[0]

    @staticmethod
    def setup(conn) -> None:
        conn.executescript(CREATE_TABLE_SQL)


def where_clause(query: Query) -> Tuple[str, List[Any]]:
    where: List[str] = []
    args: List[str] = []

    column_clause = [
        ('source', query.source),
        ('predicate', query.predicate),
        ('target', query.target),
    ]

    for column, clause in column_clause:
        if clause.is_any():
            continue

        sql_op, value = clause_to_sql(clause)
        where.append(f'{column} {sql_op} ?')
        args.append(value)

    return ' AND '.join(where), args


# FIXME:
# - `Optional[str]` because that is what `clause.Value` is.
# - Look into a better way to express this.
# - mypy catching this raises a good point about validating clauses.
# - Easy to do at runtime, however look into being able to express this
#   in the type system.
# - Perhaps this is correct here though, and other code should be aware.
# - I wonder if mypy would understand if we put logic in around this.
# - Or would I need to express it in a type.
def clause_to_sql(clause: Clause) -> Tuple[str, str]:
    if clause.type == Type.EQ:
        return '=', clause.value
    else:
        # FIXME: use a better exception
        raise Exception('Unsupported clause')
