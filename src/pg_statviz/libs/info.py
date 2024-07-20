"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2024 Jimmy Angelakos"
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
        cur.execute("""CREATE TEMP TABLE _info(hostname text);
                       COPY _info FROM PROGRAM 'hostname';
                       SELECT hostname,
                              inet_server_addr()
                       FROM _info""")
        info['hostname'], info['inet_server_addr'], info['block_size'] \
            = cur.fetchone()
        cur.close()
    except (ExternalRoutineException, InsufficientPrivilege) as e:
        conn.rollback()
        cur = conn.cursor()
        _logger.warning("Context: getting hostname")
        _logger.warning(e)
        cur.execute("""SELECT inet_server_addr(),
                              current_setting('block_size')""")
        info['inet_server_addr'], info['block_size'] = cur.fetchone()
        info['hostname'] = conn.info.host
        _logger.info(f"""Setting hostname to "{info['hostname']}" """)
        cur.close()
    return info
