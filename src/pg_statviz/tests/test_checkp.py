import numpy
from datetime import datetime, timedelta
from pg_statviz.tests.util import mock_dictrow
from pg_statviz.modules.checkp import calc_checkps, calc_checkprates

first_stats_reset = datetime.now()
second_stats_reset = datetime.now() + timedelta(seconds=30)

data = [mock_dictrow({'checkpoints_req': 15,
                      'checkpoints_timed': 140,
                      'stats_reset': first_stats_reset,
                      'snapshot_tstamp': first_stats_reset
                      + timedelta(seconds=10)}),
        mock_dictrow({'checkpoints_req': 16,
                      'checkpoints_timed': 150,
                      'stats_reset': first_stats_reset,
                      'snapshot_tstamp': first_stats_reset
                      + timedelta(seconds=20)}),
        mock_dictrow({'checkpoints_req': 17,
                      'checkpoints_timed': 160,
                      'stats_reset': second_stats_reset,
                      'snapshot_tstamp': second_stats_reset
                      + timedelta(seconds=10)}),
        mock_dictrow({'checkpoints_req': 18,
                      'checkpoints_timed': 170,
                      'stats_reset': second_stats_reset,
                      'snapshot_tstamp': second_stats_reset
                      + timedelta(seconds=20)}),
        mock_dictrow({'checkpoints_req': 20,
                      'checkpoints_timed': 190,
                      'stats_reset': second_stats_reset,
                      'snapshot_tstamp': second_stats_reset
                      + timedelta(seconds=30)})]


def test_calc_checkps():
    response = calc_checkps(data)

    req = [15, 16, 17, 18, 20]
    timed = [140, 150, 160, 170, 190]

    assert req == response['req']
    assert timed == response['timed']


def test_calc_checkprates():
    response = calc_checkprates(data)

    req = [numpy.nan, 6, numpy.nan, 6, 12]
    timed = [numpy.nan, 60, numpy.nan, 60, 120]

    numpy.testing.assert_equal(numpy.array(req),
                               numpy.array(response['req']))
    numpy.testing.assert_equal(numpy.array(timed),
                               numpy.array(response['timed']))
