"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2024 Jimmy Angelakos"
__license__ = "PostgreSQL License"

import getpass
from argh.decorators import arg
from pg_statviz.modules.buf import buf
from pg_statviz.modules.cache import cache
from pg_statviz.modules.checkp import checkp
from pg_statviz.modules.conn import conn
from pg_statviz.modules.io import io
from pg_statviz.modules.lock import lock
from pg_statviz.modules.tuple import tuple
from pg_statviz.modules.wait import wait
from pg_statviz.modules.wal import wal
from pg_statviz.modules.xact import xact
from pg_statviz.libs.dbconn import dbconn
from pg_statviz.libs.info import getinfo


@arg('-d', '--dbname', help="database name to analyze")
@arg('-h', '--host', metavar="HOSTNAME",
     help="database server host or socket directory")
@arg('-p', '--port', help="database server port")
@arg('-U', '--username', help="database user name")
@arg('-W', '--password', action='store_true',
     help="force password prompt (should happen automatically)")
@arg('-D', '--daterange', nargs=2, metavar=('FROM', 'TO'), type=str,
     help="date range to be analyzed in ISO 8601 format e.g. 2023-01-01T00:00 "
          + "2023-01-01T23:59")
@arg('-O', '--outputdir', help="output directory")
def analyze(*, dbname=getpass.getuser(), host="/var/run/postgresql", port="5432",
            username=getpass.getuser(), password=False, daterange=[],
            outputdir=None):
    "run all analysis modules"

    conn_details = {'dbname': dbname, 'user': username,
                    'password': getpass.getpass("Password: ") if password
                    else password, 'host': host, 'port': port}
    connx = dbconn(**conn_details)
    info = getinfo(connx)
    buf(daterange=daterange, outputdir=outputdir, info=info, conn=connx)
    checkp(daterange=daterange, outputdir=outputdir, info=info, conn=connx)
    cache(daterange=daterange, outputdir=outputdir, info=info, conn=connx)
    conn(daterange=daterange, outputdir=outputdir, info=info, conn=connx)
    io(daterange=daterange, outputdir=outputdir, info=info, conn=connx)
    lock(daterange=daterange, outputdir=outputdir, info=info, conn=connx)
    tuple(daterange=daterange, outputdir=outputdir, info=info, conn=connx)
    wait(daterange=daterange, outputdir=outputdir, info=info, conn=connx)
    wal(daterange=daterange, outputdir=outputdir, info=info, conn=connx)
    xact(daterange=daterange, outputdir=outputdir, info=info, conn=connx)
