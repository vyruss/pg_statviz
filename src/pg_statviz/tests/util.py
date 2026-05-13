# `psycopg.rows.DictRow` is the typing alias `Dict[str, Any]` and cannot be
# instantiated, so the mock just returns a plain dict — modules consume rows
# via __getitem__ only, which dict satisfies.
def mock_dictrow(plaindict):
    return dict(plaindict)
