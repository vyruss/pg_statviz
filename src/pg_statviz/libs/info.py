"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2026 Jimmy Angelakos"
__license__ = "PostgreSQL License"

import logging
from psycopg.errors import ExternalRoutineException, InsufficientPrivilege


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
        hostname = cur.fetchone()['hostname']
        info['hostname'] = hostname.decode() if isinstance(hostname, bytes) \
            else hostname
        cur.close()
    except (ExternalRoutineException, InsufficientPrivilege) as e:
        conn.rollback()
        cur = conn.cursor()
        _logger.warning("Context: getting hostname")
        _logger.warning(e)
        host = conn.info.host
        info['hostname'] = host.decode() if isinstance(host, bytes) else host
        _logger.info(f"""Setting hostname to "{info['hostname']}" """)
        cur.close()
    cur = conn.cursor()
    cur.execute("""SELECT current_setting('server_version') AS version,
                          pg_is_in_recovery() AS in_recovery,
                          pg_postmaster_start_time() AS started""")
    row = cur.fetchone()
    info['pg_version'] = row['version']
    info['pg_role'] = 'standby' if row['in_recovery'] else 'primary'
    info['pg_started'] = row['started']
    cur.close()
    return info


def get_settings(conn, names):
    """Return {name: value} for requested GUCs from the most recent
    pgstatviz.conf snapshot. Names absent from the snapshot are omitted.
    Returns {} if no snapshot exists or none of the names are present.

    Used by leaf modules to give the LLM the relevant configuration context
    for the chart it's analysing (e.g. shared_buffers for cache hit ratio,
    checkpoint_timeout for checkpoint analysis).
    """
    cur = conn.cursor()
    cur.execute("""SELECT conf
                   FROM pgstatviz.conf
                   ORDER BY snapshot_tstamp DESC
                   LIMIT 1""")
    row = cur.fetchone()
    cur.close()
    if not row or not row['conf']:
        return {}
    return {n: row['conf'][n] for n in names if n in row['conf']}
