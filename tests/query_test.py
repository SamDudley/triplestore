from triplestore.query import Query, Clause, Type


def test_clause_is_any_true():
    clause = Clause()

    assert clause.is_any() is True


def test_clause_is_any_false():
    clause = Clause(type=Type.EQ, value='knows')

    assert clause.is_any() is False


def test_query_is_any_true():
    query = Query()

    assert query.is_any() is True


def test_query_is_any_false():
    query = Query(
        source=Clause(type=Type.EQ, value='Sam')
    )

    assert query.is_any() is False
