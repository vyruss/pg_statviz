# `psycopg.rows.DictRow` is the typing alias `Dict[str, Any]` and cannot be
# instantiated, so the mock just returns a plain dict — modules consume rows
# via __getitem__ only, which dict satisfies.
def mock_dictrow(plaindict):
    return dict(plaindict)


class FakeCursor:
    """Returns the stored rows of whichever pgstatviz table is queried."""
    def __init__(self, tables):
        self.tables, self.rows = tables, []

    def execute(self, query, params=None):
        self.rows = next(rows for table, rows in self.tables.items()
                         if f"FROM pgstatviz.{table}" in query)

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0]

    def close(self):
        pass


class FakeConn:
    def __init__(self, tables):
        self.tables = tables

    def cursor(self):
        return FakeCursor(self.tables)
