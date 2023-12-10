"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2023 Jimmy Angelakos"
__license__ = "PostgreSQL License"

import argparse
import getpass
import logging
import psycopg2
from argh.decorators import arg
from dateutil.parser import isoparse
from matplotlib.ticker import MaxNLocator
from pg_statviz.libs import plot
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
     help="date range to be analyzed in ISO 8601 format e.g. 2023-01-01T00:00"
          + " 2023-01-01T23:59")
@arg('-O', '--outputdir', help="output directory")
@arg('--info', help=argparse.SUPPRESS)
@arg('--conn', help=argparse.SUPPRESS)
@arg('-u', '--users', help="user name(s) to plot in analysis",
     nargs='*', type=str)
def conn(dbname=getpass.getuser(), host="/var/run/postgresql", port="5432",
         username=getpass.getuser(), password=False, daterange=[],
         outputdir=None, info=None, conn=None, users=[]):
    "run connection count analysis module"

    MAX_RESULTS = 1000

    logging.basicConfig()
    _logger = logging.getLogger(__name__)
    _logger.setLevel(logging.INFO)

    if not conn:
        conn_details = {'dbname': dbname, 'user': username,
                        'password': getpass.getpass("Password: ") if password
                        else password, 'host': host, 'port': port}
        conn = dbconn(**conn_details)
    if not info:
        info = getinfo(conn)

    _logger.info("Running connection count analysis")

    if daterange:
        daterange = [isoparse(d) for d in daterange]
        if daterange[0] > daterange[1]:
            daterange = [daterange[1], daterange[0]]
    else:
        daterange = ['-infinity', 'now()']

    # Retrieve the snapshots from DB
    psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
    cur = conn.cursor()
    cur.execute("""SELECT conn_total, conn_active, conn_idle, conn_idle_trans,
                          conn_idle_trans_abort, conn_fastpath, conn_users,
                          snapshot_tstamp
                   FROM pgstatviz.conn
                   WHERE snapshot_tstamp BETWEEN %s AND %s
                   ORDER BY snapshot_tstamp DESC""",
                (daterange[0], daterange[1]))
    data = cur.fetchmany(MAX_RESULTS)
    if not data:
        raise SystemExit("No pg_statviz snapshots found in this database")

    tstamps = [t['snapshot_tstamp'] for t in data]
    total = [c['conn_total'] for c in data]
    ca = [c['conn_active'] for c in data]
    ci = [c['conn_idle'] for c in data]
    cit = [c['conn_idle_trans'] for c in data]
    cita = [c['conn_idle_trans_abort'] for c in data]
    cf = [c['conn_fastpath'] for c in data]

    # Get user names to plot
    if not users:
        for d in data:
            for c in d['conn_users']:
                if c['user'] not in users:
                    users += c['user'],

    # Connection/status count plot
    plt, fig = plot.setup()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plt.title('Connection/status count')
    plt.plot_date(tstamps, total,
                  label='total', aa=True, linestyle='solid')
    if not all(c == 0 for c in ca):
        plt.plot_date(tstamps, ca,
                      label='active', aa=True, linestyle='solid')
    if not all(c == 0 for c in ci):
        plt.plot_date(tstamps, ci,
                      label='idle', aa=True, linestyle='solid')
    if not all(c == 0 for c in cit):
        plt.plot_date(tstamps, cit,
                      label='idle in transaction', aa=True,
                      linestyle='solid')
    if not all(c == 0 for c in cita):
        plt.plot_date(tstamps, cita,
                      label='idle in transaction (aborted)', aa=True,
                      linestyle='solid')
    if not all(c == 0 for c in cf):
        plt.plot_date(tstamps, cf,
                      label='fastpath function call', aa=True,
                      linestyle='solid')
    plt.xlabel("Timestamp", fontweight='semibold')
    plt.ylabel("No. of connections", fontweight='semibold')

    fig.axes[0].set_ylim(bottom=0)
    fig.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
    fig.legend()
    fig.tight_layout()
    outfile = f"""{outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
        .replace("/", "-")}_{port}_conn_status.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)

    # Connection/user count plot
    plt, fig = plot.setup()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plt.title('Connection/user count')
    for u in users:
        uc = []
        for d in data:
            found = False
            for c in d['conn_users']:
                if c['user'] == u:
                    found = True
                    uc += c['connections'],
            if not found:
                uc += 0,
        if not all(c == 0 for c in uc):
            plt.plot_date(tstamps, uc,
                          label=u, aa=True, linestyle='solid')
    plt.xlabel("Timestamp", fontweight='semibold')
    plt.ylabel("No. of connections", fontweight='semibold')
    fig.axes[0].set_ylim(bottom=0)
    fig.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
    fig.legend()
    fig.tight_layout()
    outfile = f"""{outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
        .replace("/", "-")}_{port}_conn_user.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)
