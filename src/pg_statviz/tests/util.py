from psycopg.rows import DictRow


class MockDictRow:
    def __init__(self, plaindict):
        self.index = {key: i for i, key in enumerate(plaindict)}
        self.description = plaindict


def mock_dictrow(plaindict):
    mdr = MockDictRow(plaindict)
    dr = DictRow(mdr)
    for k, v in plaindict.items():
        dr[k] = v
    return dr
