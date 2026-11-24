import numpy
import pytest
from datetime import datetime, timedelta
from matplotlib import pyplot
from pg_statviz.tests.util import mock_dictrow
from pg_statviz.modules.io import (build_io_dataframe, calc_iostats,
                                   calc_iorates, iobytes, iokind_name,
                                   plot_iokinds)

first_stats_reset = datetime.now()
second_stats_reset = datetime.now() + timedelta(seconds=30)

data = [
    mock_dictrow(
        {
            "io_stats": [
                {
                    "reads": 3,
                    "object": "relation",
                    "writes": 0,
                    "context": "normal",
                    "backend_type": "autovacuum launcher",
                },
                {
                    "reads": 3187,
                    "object": "relation",
                    "writes": 187,
                    "context": "normal",
                    "backend_type": "autovacuum worker",
                },
                {
                    "reads": 368434,
                    "object": "relation",
                    "writes": 363979,
                    "context": "vacuum",
                    "backend_type": "autovacuum worker",
                },
                {
                    "reads": 51223,
                    "object": "relation",
                    "writes": 0,
                    "context": "bulkread",
                    "backend_type": "client backend",
                },
                {
                    "reads": 0,
                    "object": "relation",
                    "writes": 640010,
                    "context": "bulkwrite",
                    "backend_type": "client  backend",
                },
                {
                    "reads": 9058,
                    "object": "relation",
                    "writes": 31566,
                    "context": "normal",
                    "backend_type": "client backend",
                },
                {
                    "reads": 63130,
                    "object": "relation",
                    "writes": 58588,
                    "context": "vacuum",
                    "backend_type": "client backend",
                },
                {
                    "reads": 1323,
                    "object": "relation",
                    "writes": 0,
                    "context": "normal",
                    "backend_type": "background worker",
                },
                {
                    "reads": None,
                    "object": "relation",
                    "writes": 171159,
                    "context": "normal",
                    "backend_type": "background writer",
                },
                {
                    "reads": None,
                    "object": "relation",
                    "writes": 70585,
                    "context": "normal",
                    "backend_type": "checkpointer",
                },
                {
                    "reads": 535,
                    "object": "relation",
                    "writes": 1012,
                    "context": "normal",
                    "backend_type": "standalone backend",
                },
                {
                    "reads": 10,
                    "object": "relation",
                    "writes": 0,
                    "context": "vacuum",
                    "backend_type": "standalone backend",
                },
            ],
            "stats_reset": first_stats_reset,
            "snapshot_tstamp": first_stats_reset + timedelta(seconds=10),
        }
    ),
    mock_dictrow(
        {
            "io_stats": [
                {
                    "reads": 3,
                    "object": "relation",
                    "writes": 0,
                    "context": "normal",
                    "backend_type": "autovacuum launcher",
                },
                {
                    "reads": 3187,
                    "object": "relation",
                    "writes": 187,
                    "context": "normal",
                    "backend_type": "autovacuum worker",
                },
                {
                    "reads": 368434,
                    "object": "relation",
                    "writes": 363979,
                    "context": "vacuum",
                    "backend_type": "autovacuum worker",
                },
                {
                    "reads": 51223,
                    "object": "relation",
                    "writes": 0,
                    "context": "bulkread",
                    "backend_type": "client backend",
                },
                {
                    "reads": 0,
                    "object": "relation",
                    "writes": 640010,
                    "context": "bulkwrite",
                    "backend_type": "client  backend",
                },
                {
                    "reads": 10058,
                    "object": "relation",
                    "writes": 41566,
                    "context": "normal",
                    "backend_type": "client backend",
                },
                {
                    "reads": 63130,
                    "object": "relation",
                    "writes": 58588,
                    "context": "vacuum",
                    "backend_type": "client backend",
                },
                {
                    "reads": 1323,
                    "object": "relation",
                    "writes": 0,
                    "context": "normal",
                    "backend_type": "background worker",
                },
                {
                    "reads": None,
                    "object": "relation",
                    "writes": 171159,
                    "context": "normal",
                    "backend_type": "background writer",
                },
                {
                    "reads": None,
                    "object": "relation",
                    "writes": 70585,
                    "context": "normal",
                    "backend_type": "checkpointer",
                },
                {
                    "reads": 535,
                    "object": "relation",
                    "writes": 1012,
                    "context": "normal",
                    "backend_type": "standalone backend",
                },
                {
                    "reads": 10,
                    "object": "relation",
                    "writes": 0,
                    "context": "vacuum",
                    "backend_type": "standalone backend",
                },
            ],
            "stats_reset": first_stats_reset,
            "snapshot_tstamp": first_stats_reset + timedelta(seconds=20),
        }
    ),
    mock_dictrow(
        {
            "io_stats": [
                {
                    "reads": 3,
                    "object": "relation",
                    "writes": 0,
                    "context": "normal",
                    "backend_type": "autovacuum launcher",
                },
                {
                    "reads": 3187,
                    "object": "relation",
                    "writes": 187,
                    "context": "normal",
                    "backend_type": "autovacuum worker",
                },
                {
                    "reads": 368434,
                    "object": "relation",
                    "writes": 363979,
                    "context": "vacuum",
                    "backend_type": "autovacuum worker",
                },
                {
                    "reads": 51223,
                    "object": "relation",
                    "writes": 0,
                    "context": "bulkread",
                    "backend_type": "client backend",
                },
                {
                    "reads": 0,
                    "object": "relation",
                    "writes": 640010,
                    "context": "bulkwrite",
                    "backend_type": "client backend",
                },
                {
                    "reads": 11058,
                    "object": "relation",
                    "writes": 51566,
                    "context": "normal",
                    "backend_type": "client backend",
                },
                {
                    "reads": 63130,
                    "object": "relation",
                    "writes": 58588,
                    "context": "vacuum",
                    "backend_type": "client backend",
                },
                {
                    "reads": 1323,
                    "object": "relation",
                    "writes": 0,
                    "context": "normal",
                    "backend_type": "background worker",
                },
                {
                    "reads": None,
                    "object": "relation",
                    "writes": 171159,
                    "context": "normal",
                    "backend_type": "background writer",
                },
                {
                    "reads": None,
                    "object": "relation",
                    "writes": 70585,
                    "context": "normal",
                    "backend_type": "checkpointer",
                },
                {
                    "reads": 535,
                    "object": "relation",
                    "writes": 1012,
                    "context": "normal",
                    "backend_type": "standalone backend",
                },
                {
                    "reads": 10,
                    "object": "relation",
                    "writes": 0,
                    "context": "vacuum",
                    "backend_type": "standalone backend",
                },
            ],
            "stats_reset": second_stats_reset,
            "snapshot_tstamp": second_stats_reset + timedelta(seconds=10),
        }
    ),
    mock_dictrow(
        {
            "io_stats": [
                {
                    "reads": 3,
                    "object": "relation",
                    "writes": 0,
                    "context": "normal",
                    "backend_type": "autovacuum launcher",
                },
                {
                    "reads": 3187,
                    "object": "relation",
                    "writes": 187,
                    "context": "normal",
                    "backend_type": "autovacuum worker",
                },
                {
                    "reads": 368434,
                    "object": "relation",
                    "writes": 363979,
                    "context": "vacuum",
                    "backend_type": "autovacuum worker",
                },
                {
                    "reads": 51223,
                    "object": "relation",
                    "writes": 0,
                    "context": "bulkread",
                    "backend_type": "client backend",
                },
                {
                    "reads": 0,
                    "object": "relation",
                    "writes": 640010,
                    "context": "bulkwrite",
                    "backend_type": "client backend",
                },
                {
                    "reads": 12058,
                    "object": "relation",
                    "writes": 61566,
                    "context": "normal",
                    "backend_type": "client backend",
                },
                {
                    "reads": 63130,
                    "object": "relation",
                    "writes": 58588,
                    "context": "vacuum",
                    "backend_type": "client backend",
                },
                {
                    "reads": 1323,
                    "object": "relation",
                    "writes": 0,
                    "context": "normal",
                    "backend_type": "background worker",
                },
                {
                    "reads": None,
                    "object": "relation",
                    "writes": 171159,
                    "context": "normal",
                    "backend_type": "background writer",
                },
                {
                    "reads": None,
                    "object": "relation",
                    "writes": 70585,
                    "context": "normal",
                    "backend_type": "checkpointer",
                },
                {
                    "reads": 535,
                    "object": "relation",
                    "writes": 1012,
                    "context": "normal",
                    "backend_type": "standalone backend",
                },
                {
                    "reads": 10,
                    "object": "relation",
                    "writes": 0,
                    "context": "vacuum",
                    "backend_type": "standalone backend",
                },
                {
                    "reads": 11058,
                    "object": "temp relation",
                    "writes": 51566,
                    "context": "normal",
                    "backend_type": "client backend",
                },
            ],
            "stats_reset": second_stats_reset,
            "snapshot_tstamp": second_stats_reset + timedelta(seconds=20),
        }
    ),
    mock_dictrow(
        {
            "io_stats": [
                {
                    "reads": 3,
                    "object": "relation",
                    "writes": 0,
                    "context": "normal",
                    "backend_type": "autovacuum launcher",
                },
                {
                    "reads": 3187,
                    "object": "relation",
                    "writes": 187,
                    "context": "normal",
                    "backend_type": "autovacuum worker",
                },
                {
                    "reads": 368434,
                    "object": "relation",
                    "writes": 363979,
                    "context": "vacuum",
                    "backend_type": "autovacuum worker",
                },
                {
                    "reads": 51223,
                    "object": "relation",
                    "writes": 0,
                    "context": "bulkread",
                    "backend_type": "client backend",
                },
                {
                    "reads": 0,
                    "object": "relation",
                    "writes": 640010,
                    "context": "bulkwrite",
                    "backend_type": "client backend",
                },
                {
                    "reads": 13058,
                    "object": "relation",
                    "writes": 81566,
                    "context": "normal",
                    "backend_type": "client backend",
                },
                {
                    "reads": 63130,
                    "object": "relation",
                    "writes": 58588,
                    "context": "vacuum",
                    "backend_type": "client backend",
                },
                {
                    "reads": 1323,
                    "object": "relation",
                    "writes": 0,
                    "context": "normal",
                    "backend_type": "background worker",
                },
                {
                    "reads": None,
                    "object": "relation",
                    "writes": 171159,
                    "context": "normal",
                    "backend_type": "background writer",
                },
                {
                    "reads": None,
                    "object": "relation",
                    "writes": 70585,
                    "context": "normal",
                    "backend_type": "checkpointer",
                },
                {
                    "reads": 535,
                    "object": "relation",
                    "writes": 1012,
                    "context": "normal",
                    "backend_type": "standalone backend",
                },
                {
                    "reads": 10,
                    "object": "relation",
                    "writes": 0,
                    "context": "vacuum",
                    "backend_type": "standalone backend",
                },
                {
                    "reads": 110058,
                    "object": "temp relation",
                    "writes": 510566,
                    "context": "normal",
                    "backend_type": "client backend",
                },
            ],
            "stats_reset": second_stats_reset,
            "snapshot_tstamp": second_stats_reset + timedelta(seconds=30),
        }
    ),
]

# Bytes per I/O kind at each snapshot of data
iostats = {
    'read': {
        'autovacuum launcher/normal': [24576] * 5,
        'autovacuum worker/normal': [26107904] * 5,
        'autovacuum worker/vacuum': [3018211328] * 5,
        'client backend/bulkread': [419618816] * 5,
        'client  backend/bulkwrite': [0] * 5,
        'client backend/normal': [
            74203136, 82395136, 90587136, 98779136, 106971136],
        'client backend/vacuum': [517160960] * 5,
        'background worker/normal': [10838016] * 5,
        'background writer/normal': [0] * 5,
        'checkpointer/normal': [0] * 5,
        'standalone backend/normal': [4382720] * 5,
        'standalone backend/vacuum': [81920] * 5,
        'client backend/bulkwrite': [0] * 5,
        'temp relation/client backend/normal': [
            0, 0, 0, 90587136, 901595136]},
    'write': {
        'autovacuum launcher/normal': [0] * 5,
        'autovacuum worker/normal': [1531904] * 5,
        'autovacuum worker/vacuum': [2981715968] * 5,
        'client backend/bulkread': [0] * 5,
        'client  backend/bulkwrite': [5242961920, 5242961920, 0, 0, 0],
        'client backend/normal': [
            258588672, 340508672, 422428672, 504348672, 668188672],
        'client backend/vacuum': [479952896] * 5,
        'background worker/normal': [0] * 5,
        'background writer/normal': [1402134528] * 5,
        'checkpointer/normal': [578232320] * 5,
        'standalone backend/normal': [8290304] * 5,
        'standalone backend/vacuum': [0] * 5,
        'client backend/bulkwrite': [0, 0, 5242961920, 5242961920,
                                     5242961920],
        'temp relation/client backend/normal': [
            0, 0, 0, 422428672, 4182556672]}}


def test_calc_iostats():
    assert calc_iostats(data) == iostats


def test_calc_iorates():
    # NaN for the first snapshot and across the stats reset before the third
    idle = [numpy.nan, 0.0, numpy.nan, 0.0, 0.0]
    iorates = {
        'read': {**{kind: idle for kind in iostats['read']},
                 'client backend/normal': [
                     numpy.nan, 819200.0, numpy.nan, 819200.0, 819200.0],
                 'temp relation/client backend/normal': [
                     numpy.nan, 0.0, numpy.nan, 9058713.6, 81100800.0]},
        'write': {**{kind: idle for kind in iostats['write']},
                  'client backend/normal': [
                      numpy.nan, 8192000.0, numpy.nan, 8192000.0,
                      16384000.0],
                  'temp relation/client backend/normal': [
                      numpy.nan, 0.0, numpy.nan, 42242867.2, 376012800.0]}}

    numpy.testing.assert_equal(calc_iorates(data, calc_iostats(data)),
                               iorates)


def io_snapshot(seconds, *entries):
    return mock_dictrow({
        'io_stats': list(entries), 'stats_reset': first_stats_reset,
        'snapshot_tstamp': first_stats_reset + timedelta(seconds=seconds)})


@pytest.mark.parametrize('bytes_column', [False, True])
def test_calc_iorates_gives_bytes_per_second_from_blocks_or_bytes(
        bytes_column):
    def entry(blocks):
        entry = {'backend_type': 'client backend', 'object': 'relation',
                 'context': 'normal', 'reads': blocks, 'writes': blocks}
        if bytes_column:  # PG18+
            entry.update(read_bytes=blocks * 8192, write_bytes=blocks * 8192)
        return entry
    snapshots = [io_snapshot(0, entry(1000)), io_snapshot(10, entry(2000))]
    rate = [numpy.nan, 1000 * 8192 / 10]
    numpy.testing.assert_equal(calc_iorates(snapshots,
                                            calc_iostats(snapshots)),
                               {'read': {'client backend/normal': rate},
                                'write': {'client backend/normal': rate}})


def test_calc_iorates_counts_a_new_io_kind_up_from_zero():
    # snapshot_io leaves out I/O kinds that have not done any I/O yet
    snapshots = [io_snapshot(0),
                 io_snapshot(10, {'backend_type': 'client backend',
                                  'object': 'temp relation',
                                  'context': 'normal', 'reads': 1000,
                                  'writes': 0})]
    rates = calc_iorates(snapshots, calc_iostats(snapshots))
    numpy.testing.assert_equal(
        rates['read']['temp relation/client backend/normal'],
        [numpy.nan, 1000 * 8192 / 10])


def test_iokind_name_prefixes_objects_other_than_relation():
    assert [iokind_name({'backend_type': 'client backend', 'object': o,
                         'context': 'normal'})
            for o in ('relation', 'temp relation', 'wal')] == [
        'client backend/normal', 'temp relation/client backend/normal',
        'wal/client backend/normal']


def test_iobytes_reads_bytes_column_or_converts_blocks():
    assert iobytes({'writes': 2}, 'write', 8192) == 16384
    assert iobytes({'writes': 2, 'write_bytes': 16384}, 'write', 8192) == 16384
    # NULL where the operation does not apply to the I/O kind
    assert iobytes({'writes': None}, 'write', 8192) == 0
    assert iobytes({'writes': None, 'write_bytes': None}, 'write', 8192) == 0


MB, GB = 1048576, 1073741824
tstamps = [first_stats_reset + timedelta(seconds=s) for s in (0, 10, 20)]


@pytest.mark.parametrize('biggest, unit, size', [
    (512, 'KB', 1024), (45 * MB, 'MB', MB), (1.03 * GB, 'GB', GB),
    (3 * 1024 * GB, 'TB', 1024 * GB)])
def test_plot_iokinds_shows_values_in_the_unit_of_the_biggest(
        biggest, unit, size):
    _, splt = pyplot.subplots()
    plot_iokinds(splt, {'busy': [numpy.nan, biggest / 2, biggest],
                        'idle': [numpy.nan, 0, 0]},
                 tstamps, "I/O Reads", "{} read")
    pyplot.close('all')
    assert splt.get_title() == "I/O Reads"
    assert splt.get_ylabel() == f"{unit} read"
    [line] = splt.get_lines()  # the idle kind is left out
    assert line.get_label() == 'busy'
    numpy.testing.assert_allclose(line.get_ydata(),
                                  [numpy.nan, biggest / 2 / size,
                                   biggest / size])


def test_build_io_dataframe_leaves_out_series_that_stay_zero():
    series = {'read': {'a': [numpy.nan, 2 * MB, 3 * MB], 'b': [0, 0, 0]},
              'write': {'a': [0, 0, 0], 'b': [numpy.nan, 0, 1.234 * MB]}}
    df = build_io_dataframe(series, MB, 'MBps', tstamps)
    assert list(df.index) == tstamps
    assert df.to_dict('list') == {'a_read_MBps': [0, 2.0, 3.0],
                                  'b_write_MBps': [0, 0.0, 1.23]}
