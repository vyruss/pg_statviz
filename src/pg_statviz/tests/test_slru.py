import importlib
from datetime import datetime, timedelta
from matplotlib import pyplot
from pg_statviz.modules.slru import build_slru_dataframe
from pg_statviz.tests.util import FakeConn

tstamps = [datetime(2026, 1, 1) + timedelta(minutes=m) for m in (0, 15)]


def entry(name, blks_hit, blks_read):
    return {'name': name, 'blks_zeroed': 0, 'blks_hit': blks_hit,
            'blks_read': blks_read, 'blks_written': 0, 'blks_exists': 0,
            'flushes': 0, 'truncates': 0}


# SLRUs whose counters are all zero are not stored, and Subtrans is read but
# never hit
slru_stats = [[entry('Xact', 90, 10)],
              [entry('Xact', 100, 0), entry('Subtrans', 0, 4)]]


def test_build_slru_dataframe_reads_unstored_slrus_as_zero():
    df = build_slru_dataframe(slru_stats, ['Xact', 'Subtrans'], tstamps)
    assert list(df['Xact_hit_ratio']) == [90.0, 100.0]
    assert list(df['Xact_blks_read']) == [10, 0]
    assert list(df['Subtrans_blks_read']) == [0, 4]
    assert list(df['Subtrans_hit_ratio']) == [0, 0]


def test_build_slru_dataframe_handles_snapshots_without_slru_stats():
    assert build_slru_dataframe([None, []], ['Xact'], tstamps).empty


def test_slru_charts_the_hit_ratio_of_an_slru_read_but_never_hit(monkeypatch,
                                                                 tmp_path):
    module = importlib.import_module('pg_statviz.modules.slru')
    charted = []
    monkeypatch.setattr(module, 'mpclose', lambda figs: charted.extend(
        line.get_label() for line in pyplot.gcf().axes[0].get_lines()))
    rows = [{'slru_stats': ss, 'snapshot_tstamp': t}
            for ss, t in zip(slru_stats, tstamps)]
    module.slru(conn=FakeConn({'slru': rows, 'conf': [{'conf': {}}]}),
                info={'hostname': 'testhost'}, outputdir=str(tmp_path))
    assert charted == ['Xact', 'Subtrans']
