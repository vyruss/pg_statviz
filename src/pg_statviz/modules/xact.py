"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2026 Jimmy Angelakos"
__license__ = "PostgreSQL License"

import argparse
import getpass
import logging
import numpy
from argh.decorators import arg
from dateutil.parser import isoparse
from matplotlib.pyplot import close as mpclose
from matplotlib.ticker import MaxNLocator
from pandas import DataFrame
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
     help="date range to be analyzed in ISO 8601 format e.g. 2023-01-01T00:00 "
          + "2023-01-01T23:59")
@arg('-O', '--outputdir', help="output directory")
@arg('--info', help=argparse.SUPPRESS)
@arg('--conn', help=argparse.SUPPRESS)
def xact(*, dbname=getpass.getuser(), host="/var/run/postgresql", port="5432",
         username=getpass.getuser(), password=False, daterange=[],
         outputdir=None, info=None, conn=None):
    "run transaction count analysis module"

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

    _logger.info("Running transaction count analysis")

    if daterange:
        daterange = [isoparse(d) for d in daterange]
        if daterange[0] > daterange[1]:
            daterange = [daterange[1], daterange[0]]
    else:
        daterange = ['-infinity', 'now()']

    cur = conn.cursor()
    cur.execute("""SELECT xact_commit, xact_rollback, snapshot_tstamp,
                          stats_reset
                   FROM pgstatviz.db
                   WHERE snapshot_tstamp BETWEEN %s AND %s
                   ORDER BY snapshot_tstamp""",
                (daterange[0], daterange[1]))
    data = cur.fetchall()
    if not data:
        raise SystemExit("No pg_statviz snapshots found in this database")

    tstamps = [t['snapshot_tstamp'] for t in data]
    committed = [t['xact_commit'] for t in data]
    rolledback = [t['xact_rollback'] for t in data]
    xr = list(xactdiff(data))
    xactrates = {'committed': [c[0] for c in xr],
                 'rolledback': [c[1] for c in xr]}

    # Downsample if needed
    xacts_frame = DataFrame(
        data={'committed': committed, 'rolledback': rolledback},
        index=tstamps, copy=False)
    xactrates_frame = DataFrame(data=xactrates, index=tstamps, copy=False)
    if len(tstamps) > plot.MAX_POINTS:
        q = str(round(
            (tstamps[-1] - tstamps[0]).total_seconds() / plot.MAX_POINTS, 2))
        r = xacts_frame.resample(q + "s").mean()
        rr = xactrates_frame.resample(q + "s").mean()
    else:
        r = xacts_frame
        rr = xactrates_frame

    # Plot transaction count
    plt, fig = plot.setup()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plt.title("Transactions")

    plt.plot_date(r.index, r['committed'], label="Committed", aa=True,
                  linestyle='solid')
    plt.plot_date(r.index, r['rolledback'], label="Rolled back", aa=True,
                  linestyle='solid')
    plt.xlabel("Timestamp", fontweight='semibold')
    plt.ylabel("Transactions (since stats reset)", fontweight='semibold')
    fig.axes[0].set_ylim(bottom=0)
    fig.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
    fig.legend()
    fig.tight_layout()
    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_xact.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)

    # Plot transaction rates
    plt, fig = plot.setup()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plt.title("Transaction rate")
    plt.plot_date(rr.index, rr['committed'], label="Committed",
                  aa=True, linestyle='solid')
    plt.plot_date(rr.index, rr['rolledback'], label="Rolled back",
                  aa=True, linestyle='solid')
    plt.xlabel("Timestamp", fontweight='semibold')
    plt.ylabel("Avg. transactions per minute", fontweight='semibold')
    fig.axes[0].set_ylim(bottom=0)
    fig.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
    fig.legend()
    fig.tight_layout()
    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_xact_rate.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)
    mpclose('all')


# Transaction diff generator - yields tuple list of the rates in
# transactions/minute
def xactdiff(data):
    yield (numpy.nan, numpy.nan)
    for i, item in enumerate(data):
        if i + 1 < len(data):
            if data[i + 1]['stats_reset'] == data[i]['stats_reset']:
                m = (data[i + 1]['snapshot_tstamp']
                     - data[i]['snapshot_tstamp']).total_seconds() / 60
                yield (round((data[i + 1]['xact_commit']
                              - data[i]['xact_commit']) / m, 1),
                       round((data[i + 1]['xact_rollback']
                              - data[i]['xact_rollback']) / m, 1))
            else:
                yield (numpy.nan, numpy.nan)
