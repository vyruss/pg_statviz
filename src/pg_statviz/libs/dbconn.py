"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2026 Jimmy Angelakos"
__license__ = "PostgreSQL License"

import getpass
import logging
import psycopg
from psycopg.rows import dict_row


logging.basicConfig()
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)


def dbconn(dbname, user, password, host, port):

    conn_details = {'dbname': dbname, 'user': user,
                    'password': password, 'host': host, 'port': port}
    while True:
        try:
            conn = psycopg.connect(**conn_details, row_factory=dict_row)
            return conn
        except psycopg.errors.OperationalError as e:
            if "auth" in str(e):
                conn_details['password'] = getpass.getpass("Password: ")
            else:
                _logger.error(e)
                raise SystemExit("Could not connect")
