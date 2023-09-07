from datetime import datetime, timedelta
from pg_statviz.tests.util import mock_dictrow
from pg_statviz.modules.cache import calc_ratio

tstamp = datetime.now()

data = [mock_dictrow({'blks_hit': 150000, 'blks_read': 14000,
                      'snapshot_tstamp': tstamp + timedelta(seconds=10)}),
        mock_dictrow({'blks_hit': 160000, 'blks_read': 15000,
                      'snapshot_tstamp': tstamp + timedelta(seconds=20)}),
        mock_dictrow({'blks_hit': 170000, 'blks_read': 16000,
                      'snapshot_tstamp': tstamp + timedelta(seconds=30)}),
        mock_dictrow({'blks_hit': 180000, 'blks_read': 17000,
                      'snapshot_tstamp': tstamp + timedelta(seconds=40)}),
        mock_dictrow({'blks_hit': 200000, 'blks_read': 19000,
                      'snapshot_tstamp': tstamp + timedelta(seconds=50)})]


def test_calc_ratio():
    response = calc_ratio(data)

    ratio = [91.46, 91.43, 91.4, 91.37, 91.32]

    assert ratio == response
