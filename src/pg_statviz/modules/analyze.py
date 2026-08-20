"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2026 Jimmy Angelakos"
__license__ = "PostgreSQL License"

import getpass
import logging
from argh.decorators import arg
from pg_statviz.libs.ai import (AI_HELP, AI_PROVIDERS,
                                DEFAULT_AI_PROVIDER)
from pg_statviz.modules.blocking import blocking
from pg_statviz.modules.buf import buf
from pg_statviz.modules.cache import cache
from pg_statviz.modules.checkp import checkp
from pg_statviz.modules.checksum import checksum
from pg_statviz.modules.conf import conf
from pg_statviz.modules.conn import conn
from pg_statviz.modules.io import io
from pg_statviz.modules.lock import lock
from pg_statviz.modules.repl import repl
from pg_statviz.modules.slru import slru
from pg_statviz.modules.tuple import tuple
from pg_statviz.modules.wait import wait
from pg_statviz.modules.wal import wal
from pg_statviz.modules.xact import xact
from pg_statviz.libs.dbconn import dbconn
from pg_statviz.libs.html_report import finalize_index_report
from pg_statviz.libs.info import getinfo


@arg('-d', '--dbname', help="database name to analyze")
@arg('-h', '--host', metavar="HOSTNAME",
     help="database server host or socket directory")
@arg('-p', '--port', help="database server port")
@arg('-U', '--username', help="database user name")
@arg('-W', '--password', action='store_true',
     help="force password prompt (should happen automatically)")
@arg('-D', '--daterange', nargs=2, metavar=('FROM', 'TO'), type=str,
     help="date range to be analyzed in ISO 8601 format e.g. 2026-01-01T00:00 "
          + "2026-01-01T23:59")
@arg('-O', '--outputdir', help="output directory")
@arg('--ai', nargs='?', const=DEFAULT_AI_PROVIDER, default=None,
     choices=AI_PROVIDERS, metavar='PROVIDER',
     help=AI_HELP)
def analyze(*, dbname=getpass.getuser(), host="/var/run/postgresql",
            port="5432", username=getpass.getuser(), password=None,
            daterange=[], outputdir=None, ai=None):
    "run all analysis modules"

    conn_details = {'dbname': dbname, 'user': username,
                    'password': getpass.getpass("Password: ") if password
                    else password, 'host': host, 'port': port}
    connx = dbconn(**conn_details)
    info = getinfo(connx)
    _logger = logging.getLogger(__name__)
    common = dict(daterange=daterange, outputdir=outputdir, ai=ai,
                  info=info, conn=connx)
    for mod in (blocking, buf, checkp, cache, checksum, conf, conn, io,
                lock, repl, slru, tuple, wait, wal, xact):
        try:
            mod(**common)
        except SystemExit as e:
            _logger.warning(f"{mod.__name__}: {e}")
            continue
    finalize_index_report(outputdir, info, port, ai)
