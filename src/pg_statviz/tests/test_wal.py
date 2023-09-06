import numpy
from datetime import datetime, timedelta
from pg_statviz.tests.util import mock_dictrow
from pg_statviz.modules.wal import calc_wal, calc_walrates

first_stats_reset = datetime.now()
second_stats_reset = datetime.now() + timedelta(seconds=30)

data = [mock_dictrow({'wal_bytes': 15000000000,
                      'stats_reset': first_stats_reset,
                      'snapshot_tstamp': first_stats_reset
                      + timedelta(seconds=10)}),
        mock_dictrow({'wal_bytes': 16000000000,
                      'stats_reset': first_stats_reset,
                      'snapshot_tstamp': first_stats_reset
                      + timedelta(seconds=20)}),
        mock_dictrow({'wal_bytes': 17000000000,
                      'stats_reset': second_stats_reset,
                      'snapshot_tstamp': second_stats_reset
                      + timedelta(seconds=10)}),
        mock_dictrow({'wal_bytes': 18000000000,
                      'stats_reset': second_stats_reset,
                      'snapshot_tstamp': second_stats_reset
                      + timedelta(seconds=20)}),
        mock_dictrow({'wal_bytes': 20000000000,
                      'stats_reset': second_stats_reset,
                      'snapshot_tstamp': second_stats_reset
                      + timedelta(seconds=30)})]


def test_calc_wal():
    response = calc_wal(data)

    wal = [14.0, 14.9, 15.8, 16.8, 18.6]

    assert wal == response


def test_calc_walrates():
    response = calc_walrates(data)

    walrates = [numpy.nan, 95.37, numpy.nan, 95.37, 190.7]

    numpy.testing.assert_equal(numpy.array(walrates),
                               numpy.array(response))
