import importlib
import numpy
import pytest
from datetime import datetime, timedelta
from pg_statviz.tests.util import FakeConn, mock_dictrow
from pg_statviz.modules.lockwait import (calc_avg_wait, calc_deltas,
                                         calc_findings, calc_minutes)

nan = numpy.nan
first_stats_reset = datetime(2026, 1, 1, 12, 0)
second_stats_reset = first_stats_reset + timedelta(seconds=150)


def snapshot(seconds, stats_reset, *stats):
    """Build a pgstatviz.lockwait row from (locktype, waits, wait_time,
    fastpath_exceeded) tuples, leaving out lock types whose counters are all
    zero, as snapshot_lockwait does."""
    return mock_dictrow({
        'lockwait_stats': [{'locktype': lt, 'waits': w, 'wait_time': t,
                            'fastpath_exceeded': f}
                           for lt, w, t, f in stats if w or t or f],
        'stats_reset': stats_reset,
        'snapshot_tstamp': first_stats_reset + timedelta(seconds=seconds)})


# One minute apart, with a stats reset between the third and fourth snapshot
data = [snapshot(0, first_stats_reset,
                 ('relation', 0, 0.0, 10), ('transactionid', 2, 3000.0, 0)),
        snapshot(60, first_stats_reset,
                 ('relation', 3, 4500.0, 70), ('transactionid', 2, 3000.0, 0)),
        snapshot(120, first_stats_reset,
                 ('relation', 3, 4500.0, 70), ('transactionid', 4, 5000.0, 0)),
        snapshot(180, second_stats_reset,
                 ('relation', 0, 0.0, 0), ('transactionid', 1, 1200.0, 0)),
        snapshot(240, second_stats_reset,
                 ('relation', 1, 2000.0, 30), ('transactionid', 1, 1200.0, 0))]


def assert_columns(frame, expected):
    assert list(frame.columns) == list(expected)
    for col, values in expected.items():
        numpy.testing.assert_equal(frame[col].to_numpy(), numpy.array(values))


def test_calc_deltas_differences_counters_and_skips_resets():
    # NaN for the first snapshot and for the first one after a reset
    assert_columns(calc_deltas(data, 'waits'),
                   {'relation': [nan, 3, 0, nan, 1],
                    'transactionid': [nan, 0, 2, nan, 0]})


def test_calc_deltas_reads_unstored_lock_types_as_zero():
    # A lock type is stored from the first snapshot its counters move
    sparse = [snapshot(0, first_stats_reset, ('tuple', 0, 0.0, 0)),
              snapshot(60, first_stats_reset, ('tuple', 2, 2500.0, 0))]
    assert sparse[0]['lockwait_stats'] == []
    assert_columns(calc_deltas(sparse, 'waits'), {'tuple': [nan, 2]})


def test_calc_deltas_is_indexed_by_snapshot_time():
    deltas = calc_deltas(data, 'waits')
    assert list(deltas.index) == [d['snapshot_tstamp'] for d in data]


def test_calc_minutes_is_interval_length_except_across_resets():
    assert_columns(calc_minutes(data).to_frame('m'),
                   {'m': [nan, 1, 1, nan, 1]})
    two = [snapshot(0, first_stats_reset), snapshot(30, first_stats_reset)]
    assert_columns(calc_minutes(two).to_frame('m'), {'m': [nan, 0.5]})


def test_calc_avg_wait_is_time_per_wait_in_each_interval():
    # NaN where nothing waited, never a division by zero
    assert_columns(calc_avg_wait(calc_deltas(data, 'waits'),
                                 calc_deltas(data, 'wait_time')),
                   {'relation': [nan, 1500, nan, nan, 2000],
                    'transactionid': [nan, nan, 1000, nan, nan]})


def run_lockwait(rows, monkeypatch, tmp_path):
    """Run the lockwait module and return the frames it hands to the AI
    analysis, by chart title."""
    module = importlib.import_module('pg_statviz.modules.lockwait')
    frames = {}
    monkeypatch.setattr(module, 'run_chart_analysis',
                        lambda sections, ai, df, title, **kwargs:
                        frames.update({title: df}))
    module.lockwait(conn=FakeConn({'lockwait': rows, 'conf': [{'conf': {}}]}),
                    info={'hostname': 'testhost'}, outputdir=str(tmp_path),
                    ai='claude')
    return frames


def test_lockwait_charts_waits_and_overflows_per_minute(monkeypatch,
                                                        tmp_path):
    frames = run_lockwait(data, monkeypatch, tmp_path)
    assert_columns(frames['Lock Waits'],
                   {'Total waits/min': [nan, 3, 2, nan, 1],
                    'relation waits/min': [nan, 3, 0, nan, 1],
                    'transactionid waits/min': [nan, 0, 2, nan, 0],
                    'relation ms per wait': [nan, 1500, nan, nan, 2000],
                    'transactionid ms per wait': [nan, nan, 1000, nan, nan]})
    assert_columns(frames['Fast-path Lock Slot Overflows'],
                   {'relation overflows/min': [nan, 60, 0, nan, 30]})


def test_lockwait_charts_a_zero_total_when_nothing_waited(monkeypatch,
                                                          tmp_path):
    # No lock type is stored, and the stats reset still breaks the line
    quiet = [snapshot(60 * i, d['stats_reset']) for i, d in enumerate(data)]
    assert_columns(run_lockwait(quiet, monkeypatch, tmp_path)['Lock Waits'],
                   {'Total waits/min': [nan, 0, 0, nan, 0]})


def test_lockwait_downsampling_weighs_each_wait_equally(monkeypatch,
                                                        tmp_path):
    # 201 snapshots a minute apart are summed into two-minute buckets. The
    # bucket from 12:02 has one wait of 1000 ms and then 99 waits of 10 ms.
    def counters(minute):  # waits, wait_time
        if minute < 2:
            return 0, 0.0
        if minute == 2:
            return 1, 1000.0
        return 100, 1990.0
    rows = [snapshot(60 * m, first_stats_reset, ('relation', *counters(m), 0))
            for m in range(201)]
    lock_waits = run_lockwait(rows, monkeypatch, tmp_path)['Lock Waits']
    bucket = lock_waits.loc[first_stats_reset + timedelta(minutes=2)]
    assert bucket['relation ms per wait'] == pytest.approx(1990 / 100)
    assert bucket['relation waits/min'] == pytest.approx(100 / 2)


def findings(data):
    return calc_findings(calc_deltas(data, 'waits'),
                         calc_deltas(data, 'wait_time'))


def test_calc_findings_clean_when_nothing_waited():
    quiet = [snapshot(0, first_stats_reset, ('relation', 5, 9000.0, 4)),
             snapshot(60, first_stats_reset, ('relation', 5, 9000.0, 9))]
    assert findings(quiet) == []


def test_calc_findings_clean_with_a_single_snapshot():
    assert findings(data[:1]) == []


def test_calc_findings_warns_on_occasional_waits():
    occasional = [snapshot(0, first_stats_reset, ('relation', 0, 0.0, 0)),
                  snapshot(60, first_stats_reset, ('relation', 2, 2600.0, 0)),
                  snapshot(120, first_stats_reset, ('relation', 2, 2600.0, 0)),
                  snapshot(180, first_stats_reset, ('relation', 2, 2600.0, 0))]
    found = findings(occasional)
    assert len(found) == 1
    assert found[0]['severity'] == 'WARNING'
    assert '1300' in found[0]['message']  # average ms per wait


def test_calc_findings_escalates_when_waits_are_sustained():
    # Waits in every interval, and the reset interval is not counted
    found = findings(data)
    assert len(found) == 1
    assert found[0]['severity'] == 'CRITICAL'
    assert '6 lock wait' in found[0]['message']
    assert 'relation' in found[0]['message']  # most waits
