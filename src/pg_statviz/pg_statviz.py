#!/usr/bin/env python3
"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2023 Jimmy Angelakos"
__license__ = "PostgreSQL License"
__version__ = "0.5"

import sys
from argh import ArghParser
from pg_statviz.modules.analyze import analyze
from pg_statviz.modules.buf import buf
from pg_statviz.modules.cache import cache
from pg_statviz.modules.checkp import checkp
from pg_statviz.modules.conn import conn
from pg_statviz.modules.lock import lock
from pg_statviz.modules.tuple import tuple
from pg_statviz.modules.wait import wait
from pg_statviz.modules.wal import wal


# Python version check
if sys.version_info < (3, 9):
    raise SystemExit("This program requires Python 3.9 or later")


def main():
    # CLI parser
    p = ArghParser(add_help=False)
    p.add_argument("--help", action="help")
    p.add_argument('--version', action='version',
                   version=f"pg_statviz {__version__}")

    p.add_commands([analyze, buf, cache, checkp, conn, lock, tuple, wait,
                    wal])
    p.set_default_command(analyze)
    p.dispatch()


if __name__ == "__main__":
    main()
