"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2024 Jimmy Angelakos"
__license__ = "PostgreSQL License"

import getpass
import logging
import psycopg2
from psycopg2.extras import DictCursor


logging.basicConfig()
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)


def dbconn(dbname, user, password, host, port):

    conn_details = {'dbname': dbname, 'user': user,
                    'password': password, 'host': host, 'port': port}
    while True:
        try:
            conn = psycopg2.connect(**conn_details, cursor_factory=DictCursor)
            return conn
        except psycopg2.errors.OperationalError as e:
            if "auth" in str(e):
                conn_details['password'] = getpass.getpass("Password: ")
            else:
                _logger.error(e)
                raise SystemExit("Could not connect")
