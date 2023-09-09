import numpy
from datetime import datetime, timedelta
from pg_statviz.tests.util import mock_dictrow
from pg_statviz.modules.io import calc_iostats, calc_iorates

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

iostats = [
    [
        {
            "reads": 24576,
            "object": "relation",
            "writes": 0,
            "context": "normal",
            "backend_type": "autovacuum launcher",
        },
        {
            "reads": 26107904,
            "object": "relation",
            "writes": 1531904,
            "context": "normal",
            "backend_type": "autovacuum worker",
        },
        {
            "reads": 3018211328,
            "object": "relation",
            "writes": 2981715968,
            "context": "vacuum",
            "backend_type": "autovacuum worker",
        },
        {
            "reads": 419618816,
            "object": "relation",
            "writes": 0,
            "context": "bulkread",
            "backend_type": "client backend",
        },
        {
            "reads": 0,
            "object": "relation",
            "writes": 5242961920,
            "context": "bulkwrite",
            "backend_type": "client  backend",
        },
        {
            "reads": 74203136,
            "object": "relation",
            "writes": 258588672,
            "context": "normal",
            "backend_type": "client backend",
        },
        {
            "reads": 517160960,
            "object": "relation",
            "writes": 479952896,
            "context": "vacuum",
            "backend_type": "client backend",
        },
        {
            "reads": 10838016,
            "object": "relation",
            "writes": 0,
            "context": "normal",
            "backend_type": "background worker",
        },
        {
            "reads": None,
            "object": "relation",
            "writes": 1402134528,
            "context": "normal",
            "backend_type": "background writer",
        },
        {
            "reads": None,
            "object": "relation",
            "writes": 578232320,
            "context": "normal",
            "backend_type": "checkpointer",
        },
        {
            "reads": 4382720,
            "object": "relation",
            "writes": 8290304,
            "context": "normal",
            "backend_type": "standalone backend",
        },
        {
            "reads": 81920,
            "object": "relation",
            "writes": 0,
            "context": "vacuum",
            "backend_type": "standalone backend",
        },
    ],
    [
        {
            "reads": 24576,
            "object": "relation",
            "writes": 0,
            "context": "normal",
            "backend_type": "autovacuum launcher",
        },
        {
            "reads": 26107904,
            "object": "relation",
            "writes": 1531904,
            "context": "normal",
            "backend_type": "autovacuum worker",
        },
        {
            "reads": 3018211328,
            "object": "relation",
            "writes": 2981715968,
            "context": "vacuum",
            "backend_type": "autovacuum worker",
        },
        {
            "reads": 419618816,
            "object": "relation",
            "writes": 0,
            "context": "bulkread",
            "backend_type": "client backend",
        },
        {
            "reads": 0,
            "object": "relation",
            "writes": 5242961920,
            "context": "bulkwrite",
            "backend_type": "client  backend",
        },
        {
            "reads": 82395136,
            "object": "relation",
            "writes": 340508672,
            "context": "normal",
            "backend_type": "client backend",
        },
        {
            "reads": 517160960,
            "object": "relation",
            "writes": 479952896,
            "context": "vacuum",
            "backend_type": "client backend",
        },
        {
            "reads": 10838016,
            "object": "relation",
            "writes": 0,
            "context": "normal",
            "backend_type": "background worker",
        },
        {
            "reads": None,
            "object": "relation",
            "writes": 1402134528,
            "context": "normal",
            "backend_type": "background writer",
        },
        {
            "reads": None,
            "object": "relation",
            "writes": 578232320,
            "context": "normal",
            "backend_type": "checkpointer",
        },
        {
            "reads": 4382720,
            "object": "relation",
            "writes": 8290304,
            "context": "normal",
            "backend_type": "standalone backend",
        },
        {
            "reads": 81920,
            "object": "relation",
            "writes": 0,
            "context": "vacuum",
            "backend_type": "standalone backend",
        },
    ],
    [
        {
            "reads": 24576,
            "object": "relation",
            "writes": 0,
            "context": "normal",
            "backend_type": "autovacuum launcher",
        },
        {
            "reads": 26107904,
            "object": "relation",
            "writes": 1531904,
            "context": "normal",
            "backend_type": "autovacuum worker",
        },
        {
            "reads": 3018211328,
            "object": "relation",
            "writes": 2981715968,
            "context": "vacuum",
            "backend_type": "autovacuum worker",
        },
        {
            "reads": 419618816,
            "object": "relation",
            "writes": 0,
            "context": "bulkread",
            "backend_type": "client backend",
        },
        {
            "reads": 0,
            "object": "relation",
            "writes": 5242961920,
            "context": "bulkwrite",
            "backend_type": "client backend",
        },
        {
            "reads": 90587136,
            "object": "relation",
            "writes": 422428672,
            "context": "normal",
            "backend_type": "client backend",
        },
        {
            "reads": 517160960,
            "object": "relation",
            "writes": 479952896,
            "context": "vacuum",
            "backend_type": "client backend",
        },
        {
            "reads": 10838016,
            "object": "relation",
            "writes": 0,
            "context": "normal",
            "backend_type": "background worker",
        },
        {
            "reads": None,
            "object": "relation",
            "writes": 1402134528,
            "context": "normal",
            "backend_type": "background writer",
        },
        {
            "reads": None,
            "object": "relation",
            "writes": 578232320,
            "context": "normal",
            "backend_type": "checkpointer",
        },
        {
            "reads": 4382720,
            "object": "relation",
            "writes": 8290304,
            "context": "normal",
            "backend_type": "standalone backend",
        },
        {
            "reads": 81920,
            "object": "relation",
            "writes": 0,
            "context": "vacuum",
            "backend_type": "standalone backend",
        },
    ],
    [
        {
            "reads": 24576,
            "object": "relation",
            "writes": 0,
            "context": "normal",
            "backend_type": "autovacuum launcher",
        },
        {
            "reads": 26107904,
            "object": "relation",
            "writes": 1531904,
            "context": "normal",
            "backend_type": "autovacuum worker",
        },
        {
            "reads": 3018211328,
            "object": "relation",
            "writes": 2981715968,
            "context": "vacuum",
            "backend_type": "autovacuum worker",
        },
        {
            "reads": 419618816,
            "object": "relation",
            "writes": 0,
            "context": "bulkread",
            "backend_type": "client backend",
        },
        {
            "reads": 0,
            "object": "relation",
            "writes": 5242961920,
            "context": "bulkwrite",
            "backend_type": "client backend",
        },
        {
            "reads": 98779136,
            "object": "relation",
            "writes": 504348672,
            "context": "normal",
            "backend_type": "client backend",
        },
        {
            "reads": 517160960,
            "object": "relation",
            "writes": 479952896,
            "context": "vacuum",
            "backend_type": "client backend",
        },
        {
            "reads": 10838016,
            "object": "relation",
            "writes": 0,
            "context": "normal",
            "backend_type": "background worker",
        },
        {
            "reads": None,
            "object": "relation",
            "writes": 1402134528,
            "context": "normal",
            "backend_type": "background writer",
        },
        {
            "reads": None,
            "object": "relation",
            "writes": 578232320,
            "context": "normal",
            "backend_type": "checkpointer",
        },
        {
            "reads": 4382720,
            "object": "relation",
            "writes": 8290304,
            "context": "normal",
            "backend_type": "standalone backend",
        },
        {
            "reads": 81920,
            "object": "relation",
            "writes": 0,
            "context": "vacuum",
            "backend_type": "standalone backend",
        },
        {
            "reads": 90587136,
            "object": "temp relation",
            "writes": 422428672,
            "context": "normal",
            "backend_type": "client backend",
        },
    ],
    [
        {
            "reads": 24576,
            "object": "relation",
            "writes": 0,
            "context": "normal",
            "backend_type": "autovacuum launcher",
        },
        {
            "reads": 26107904,
            "object": "relation",
            "writes": 1531904,
            "context": "normal",
            "backend_type": "autovacuum worker",
        },
        {
            "reads": 3018211328,
            "object": "relation",
            "writes": 2981715968,
            "context": "vacuum",
            "backend_type": "autovacuum worker",
        },
        {
            "reads": 419618816,
            "object": "relation",
            "writes": 0,
            "context": "bulkread",
            "backend_type": "client backend",
        },
        {
            "reads": 0,
            "object": "relation",
            "writes": 5242961920,
            "context": "bulkwrite",
            "backend_type": "client backend",
        },
        {
            "reads": 106971136,
            "object": "relation",
            "writes": 668188672,
            "context": "normal",
            "backend_type": "client backend",
        },
        {
            "reads": 517160960,
            "object": "relation",
            "writes": 479952896,
            "context": "vacuum",
            "backend_type": "client backend",
        },
        {
            "reads": 10838016,
            "object": "relation",
            "writes": 0,
            "context": "normal",
            "backend_type": "background worker",
        },
        {
            "reads": None,
            "object": "relation",
            "writes": 1402134528,
            "context": "normal",
            "backend_type": "background writer",
        },
        {
            "reads": None,
            "object": "relation",
            "writes": 578232320,
            "context": "normal",
            "backend_type": "checkpointer",
        },
        {
            "reads": 4382720,
            "object": "relation",
            "writes": 8290304,
            "context": "normal",
            "backend_type": "standalone backend",
        },
        {
            "reads": 81920,
            "object": "relation",
            "writes": 0,
            "context": "vacuum",
            "backend_type": "standalone backend",
        },
        {
            "reads": 901595136,
            "object": "temp relation",
            "writes": 4182556672,
            "context": "normal",
            "backend_type": "client backend",
        },
    ],
]

iokinds = [
    {
        "backend_type": "autovacuum launcher",
        "object": "relation",
        "context": "normal",
    },
    {
        "backend_type": "autovacuum worker",
        "object": "relation",
        "context": "normal",
    },
    {
        "backend_type": "autovacuum worker",
        "object": "relation",
        "context": "vacuum",
    },
    {
        "backend_type": "client backend",
        "object": "relation",
        "context": "bulkread"},
    {
        "backend_type": "client  backend",
        "object": "relation",
        "context": "bulkwrite",
    },
    {
        "backend_type": "client backend",
        "object": "relation",
        "context": "normal"},
    {
        "backend_type": "client backend",
        "object": "relation",
        "context": "vacuum"},
    {
        "backend_type": "background worker",
        "object": "relation",
        "context": "normal",
    },
    {
        "backend_type": "background writer",
        "object": "relation",
        "context": "normal",
    },
    {
        "backend_type": "checkpointer",
        "object": "relation",
        "context": "normal"},
    {
        "backend_type": "standalone backend",
        "object": "relation",
        "context": "normal",
    },
    {
        "backend_type": "standalone backend",
        "object": "relation",
        "context": "vacuum",
    },
    {
        "backend_type": "client backend",
        "object": "relation",
        "context": "bulkwrite",
    },
    {
        "backend_type": "client backend",
        "object": "temp relation",
        "context": "normal",
    }
]


def test_calc_iostats():
    response = calc_iostats(data)

    assert iostats == response[0]
    assert iokinds == response[1]


def test_calc_iorates():
    response = calc_iorates(data, iokinds)

    iorates = {
        "reads": {
            "autovacuum launcher/normal": [
                numpy.nan, 0.0, numpy.nan, 0.0, 0.0],
            "autovacuum worker/normal": [numpy.nan, 0.0, numpy.nan, 0.0, 0.0],
            "autovacuum worker/vacuum": [numpy.nan, 0.0, numpy.nan, 0.0, 0.0],
            "client backend/bulkread": [numpy.nan, 0.0, numpy.nan, 0.0, 0.0],
            "client  backend/bulkwrite": [
                numpy.nan, numpy.nan, numpy.nan, numpy.nan, numpy.nan],
            "client backend/normal": [
                numpy.nan, numpy.nan, numpy.nan, numpy.nan, 81100800.0],
            "client backend/vacuum": [numpy.nan, 0.0, numpy.nan, 0.0, 0.0],
            "background worker/normal": [numpy.nan, 0.0, numpy.nan, 0.0, 0.0],
            "background writer/normal": [
                numpy.nan, numpy.nan, numpy.nan, numpy.nan, numpy.nan],
            "checkpointer/normal": [
                numpy.nan, numpy.nan, numpy.nan, numpy.nan, numpy.nan],
            "standalone backend/normal": [numpy.nan, 0.0, numpy.nan, 0.0, 0.0],
            "standalone backend/vacuum": [numpy.nan, 0.0, numpy.nan, 0.0, 0.0],
            "client backend/bulkwrite": [
                numpy.nan, numpy.nan, numpy.nan, numpy.nan, numpy.nan, ]},
        "writes": {
            "autovacuum launcher/normal": [
                numpy.nan, numpy.nan, numpy.nan, numpy.nan, numpy.nan,
            ],
            "autovacuum worker/normal": [numpy.nan, 0.0, numpy.nan, 0.0, 0.0],
            "autovacuum worker/vacuum": [numpy.nan, 0.0, numpy.nan, 0.0, 0.0],
            "client backend/bulkread": [
                numpy.nan, numpy.nan, numpy.nan, numpy.nan, numpy.nan,
            ],
            "client  backend/bulkwrite": [
                numpy.nan, 0.0, numpy.nan, numpy.nan, numpy.nan],
            "client backend/normal": [
                numpy.nan, numpy.nan, numpy.nan, numpy.nan, 376012800.0],
            "client backend/vacuum": [numpy.nan, 0.0, numpy.nan, 0.0, 0.0],
            "background worker/normal": [
                numpy.nan, numpy.nan, numpy.nan, numpy.nan, numpy.nan,
            ],
            "background writer/normal": [numpy.nan, 0.0, numpy.nan, 0.0, 0.0],
            "checkpointer/normal": [numpy.nan, 0.0, numpy.nan, 0.0, 0.0],
            "standalone backend/normal": [numpy.nan, 0.0, numpy.nan, 0.0, 0.0],
            "standalone backend/vacuum": [
                numpy.nan, numpy.nan, numpy.nan, numpy.nan, numpy.nan],
            "client backend/bulkwrite": [
                numpy.nan, numpy.nan, numpy.nan, 0.0, 0.0]}}

    numpy.testing.assert_equal(numpy.array(iorates), numpy.array(response))
