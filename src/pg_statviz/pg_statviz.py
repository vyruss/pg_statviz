#!/usr/bin/env python3
"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2026 Jimmy Angelakos"
__license__ = "PostgreSQL License"
__version__ = "1.2"

import sys
from argh import ArghParser
from argh.utils import get_subparsers
from pg_statviz.modules.analyze import analyze
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


# Python version check
if sys.version_info < (3, 11):
    raise SystemExit("This program requires Python 3.11 or later")


HELP_FLAGS = ('-?', '--help')
HELP_TEXT = "show this help, then exit"


def main():
    # CLI parser. add_help is off at both levels so that -h stays free for
    # --host, as in psql and the other PostgreSQL client tools; they expose
    # help as -? / --help instead.
    p = ArghParser(add_help=False)
    p.add_argument(*HELP_FLAGS, action='help', help=HELP_TEXT)
    p.add_argument('--version', action='version',
                   version=f"pg_statviz {__version__}")

    p.add_commands([analyze, blocking, buf, cache, checkp, checksum, conf,
                    conn, io, lock, repl, slru, tuple, wait, wal,
                    xact],
                   func_kwargs={'add_help': False})
    for subparser in get_subparsers(p).choices.values():
        subparser.add_argument(*HELP_FLAGS, action='help', help=HELP_TEXT)
    p.set_default_command(analyze)
    p.dispatch()


if __name__ == "__main__":
    main()
