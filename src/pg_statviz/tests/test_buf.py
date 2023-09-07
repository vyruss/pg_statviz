import numpy
from datetime import datetime, timedelta
from pg_statviz.tests.util import mock_dictrow
from pg_statviz.modules.buf import calc_buffers, calc_bufrates

first_stats_reset = datetime.now()
second_stats_reset = datetime.now() + timedelta(seconds=30)

data = [mock_dictrow({'buffers_checkpoint': 150000,
                      'buffers_clean': 140000,
                      'buffers_backend': 130000,
                      'stats_reset': first_stats_reset,
                      'snapshot_tstamp': first_stats_reset
                      + timedelta(seconds=10)}),
        mock_dictrow({'buffers_checkpoint': 160000,
                      'buffers_clean': 150000,
                      'buffers_backend': 140000,
                      'stats_reset': first_stats_reset,
                      'snapshot_tstamp': first_stats_reset
                      + timedelta(seconds=20)}),
        mock_dictrow({'buffers_checkpoint': 170000,
                      'buffers_clean': 160000,
                      'buffers_backend': 150000,
                      'stats_reset': second_stats_reset,
                      'snapshot_tstamp': second_stats_reset
                      + timedelta(seconds=10)}),
        mock_dictrow({'buffers_checkpoint': 180000,
                      'buffers_clean': 170000,
                      'buffers_backend': 160000,
                      'stats_reset': second_stats_reset,
                      'snapshot_tstamp': second_stats_reset
                      + timedelta(seconds=20)}),
        mock_dictrow({'buffers_checkpoint': 200000,
                      'buffers_clean': 190000,
                      'buffers_backend': 180000,
                      'stats_reset': second_stats_reset,
                      'snapshot_tstamp': second_stats_reset
                      + timedelta(seconds=30)})]


def test_calc_buffers():
    response = calc_buffers(data)

    total = [3.2, 3.4, 3.7, 3.9, 4.3]
    checkpoints = [1.1, 1.2, 1.3, 1.4, 1.5]
    bgwriter = [1.1, 1.1, 1.2, 1.3, 1.4]
    backends = [1.0, 1.1, 1.1, 1.2, 1.4]

    assert total == response['total']
    assert checkpoints == response['checkpoints']
    assert bgwriter == response['bgwriter']
    assert backends == response['backends']


def test_calc_bufrates():
    response = calc_bufrates(data)

    total = [numpy.nan, 23.4, numpy.nan, 23.4, 46.9]
    checkpoints = [numpy.nan, 7.8, numpy.nan, 7.8, 15.6]
    bgwriter = [numpy.nan, 7.8, numpy.nan, 7.8, 15.6]
    backends = [numpy.nan, 7.8, numpy.nan, 7.8, 15.6]

    numpy.testing.assert_equal(numpy.array(total),
                               numpy.array(response['total']))
    numpy.testing.assert_equal(numpy.array(checkpoints),
                               numpy.array(response['checkpoints']))
    numpy.testing.assert_equal(numpy.array(bgwriter),
                               numpy.array(response['bgwriter']))
    numpy.testing.assert_equal(numpy.array(backends),
                               numpy.array(response['backends']))
