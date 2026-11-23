"""Each module hands the settings that explain its charts to the AI
analysis, read from the latest pgstatviz.conf snapshot."""
import importlib
import pytest
from datetime import datetime, timedelta
from pg_statviz.tests.util import FakeConn

t0 = datetime(2026, 1, 1)


def snapshots(**columns):
    return [{'snapshot_tstamp': t0 + timedelta(minutes=15 * i),
             'stats_reset': t0, **{k: v[i] for k, v in columns.items()}}
            for i in range(3)]


slru_entry = {'name': 'transaction', 'blks_zeroed': 0, 'blks_hit': 90,
              'blks_read': 10, 'blks_written': 0, 'blks_exists': 0,
              'flushes': 0, 'truncates': 0}
GB = 1073741824


def io_entry(gb):
    return {'backend_type': 'client backend', 'object': 'relation',
            'context': 'normal', 'reads': gb * GB // 8192,
            'read_bytes': gb * GB, 'writes': gb * GB // 16384,
            'write_bytes': gb * GB // 2}


def lockwait_entry(waits):
    return {'locktype': 'relation', 'waits': waits,
            'wait_time': waits * 1500.0, 'fastpath_exceeded': waits}


MODULES = {
    'slru': (snapshots(slru_stats=[[slru_entry]] * 3),
             ['transaction_buffers', 'subtransaction_buffers',
              'multixact_offset_buffers', 'multixact_member_buffers',
              'commit_timestamp_buffers', 'notify_buffers',
              'serializable_buffers']),
    'io': (snapshots(io_stats=[[io_entry(gb)] for gb in (2, 3, 5)],
                     block_size=[8192] * 3),
           ['io_method', 'io_workers', 'io_min_workers', 'io_max_workers',
            'io_worker_idle_timeout', 'io_worker_launch_interval']),
    'wal': (snapshots(wal_bytes=[GB, 2 * GB, 4 * GB]),
            ['wal_compression', 'full_page_writes']),
    'repl': (snapshots(standby_lag=[[{'application_name': 'standby1',
                                      'lag_bytes': 100}]] * 3,
                       slot_stats=[[{'slot_name': 'slot1',
                                     'wal_bytes': 2048}]] * 3),
             ['max_slot_wal_keep_size', 'wal_keep_size',
              'idle_replication_slot_timeout']),
    'lockwait': (snapshots(lockwait_stats=[[lockwait_entry(w)]
                                           for w in (1, 2, 4)]),
                 ['deadlock_timeout', 'log_lock_waits',
                  'max_locks_per_transaction']),
}


@pytest.mark.parametrize('name', MODULES)
def test_module_gives_its_settings_to_the_ai_analysis(name, tmp_path,
                                                      monkeypatch):
    rows, expected = MODULES[name]
    conf = {setting: f'{setting} value' for setting in expected}
    conn = FakeConn({name: rows, 'conf': [{'conf': conf}]})
    module = importlib.import_module(f'pg_statviz.modules.{name}')
    received = []
    monkeypatch.setattr(
        module, 'run_chart_analysis',
        lambda *args, **kwargs: received.append(kwargs.get('settings')))
    getattr(module, name)(conn=conn, info={'hostname': 'testhost'},
                          outputdir=str(tmp_path), ai='claude')
    assert received, f"{name} made no AI analysis call"
    assert all(settings == conf for settings in received)
