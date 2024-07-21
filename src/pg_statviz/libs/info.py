"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2024 Jimmy Angelakos"
__license__ = "PostgreSQL License"

import logging
from psycopg.errors import ExternalRoutineException, InsufficientPrivilege
from psycopg.rows import dict_row


logging.basicConfig()
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)


def getinfo(conn):

    info = {}
    try:
        cur = conn.cursor()
        cur.execute("""SELECT 1
                       FROM pg_extension
                       WHERE extname='pg_statviz'""")
        if not cur.fetchone():
            raise SystemExit("pg_statviz extension is not installed in this "
                             + "database")
        cur.execute("""CREATE TEMP TABLE _info(hostname text)""")
        cur.execute("""COPY _info
                       FROM PROGRAM 'hostname'""")
        cur.execute("""SELECT hostname
                       FROM _info""")
        info['hostname'] = cur.fetchone()['hostname']
        cur.close()
    except (ExternalRoutineException, InsufficientPrivilege) as e:
        conn.rollback()
        cur = conn.cursor()
        _logger.warning("Context: getting hostname")
        _logger.warning(e)
        info['hostname'] = conn.info.host
        _logger.info(f"""Setting hostname to "{info['hostname']}" """)
        cur.close()
    return info
