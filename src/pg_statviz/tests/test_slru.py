from datetime import datetime, timedelta
from pg_statviz.modules.slru import build_slru_dataframe

tstamps = [datetime(2026, 1, 1) + timedelta(minutes=m) for m in (0, 15)]


def entry(name, blks_hit, blks_read):
    return {'name': name, 'blks_zeroed': 0, 'blks_hit': blks_hit,
            'blks_read': blks_read, 'blks_written': 0, 'blks_exists': 0,
            'flushes': 0, 'truncates': 0}


def test_build_slru_dataframe_reads_unstored_slrus_as_zero():
    # SLRUs whose counters are all zero are not stored
    slru_stats = [[entry('Xact', 90, 10)],
                  [entry('Xact', 100, 0), entry('Subtrans', 0, 4)]]
    df = build_slru_dataframe(slru_stats, ['Xact', 'Subtrans'], tstamps)
    assert list(df['Xact_hit_ratio']) == [90.0, 100.0]
    assert list(df['Xact_blks_read']) == [10, 0]
    assert list(df['Subtrans_blks_read']) == [0, 4]
    assert 'Subtrans_hit_ratio' not in df.columns  # never hit


def test_build_slru_dataframe_handles_snapshots_without_slru_stats():
    assert build_slru_dataframe([None, []], ['Xact'], tstamps).empty
