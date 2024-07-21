"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2024 Jimmy Angelakos"
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
def checkp(*, dbname=getpass.getuser(), host="/var/run/postgresql",
           port="5432", username=getpass.getuser(), password=False,
           daterange=[], outputdir=None, info=None, conn=None):
    "run checkpoint analysis module"

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

    _logger.info("Running checkpoint analysis")

    if daterange:
        daterange = [isoparse(d) for d in daterange]
        if daterange[0] > daterange[1]:
            daterange = [daterange[1], daterange[0]]
    else:
        daterange = ['-infinity', 'now()']

    # Retrieve the snapshots from DB
    cur = conn.cursor()
    cur.execute("""SELECT checkpoints_req, checkpoints_timed,
                          snapshot_tstamp, stats_reset
                   FROM pgstatviz.buf
                   WHERE snapshot_tstamp BETWEEN %s AND %s
                   ORDER BY snapshot_tstamp""",
                (daterange[0], daterange[1]))
    data = cur.fetchall()
    if not data:
        raise SystemExit("No pg_statviz snapshots found in this database")

    tstamps = [t['snapshot_tstamp'] for t in data]
    checkps = calc_checkps(data)
    checkprates = calc_checkprates(data)

    # Downsample if needed
    checkps_frame = DataFrame(data=checkps, index=tstamps, copy=False)
    checkprates_frame = DataFrame(data=checkprates, index=tstamps, copy=False)
    if len(tstamps) > plot.MAX_POINTS:
        q = str(round(
            (tstamps[-1] - tstamps[0]).total_seconds() / plot.MAX_POINTS, 2))
        r = checkps_frame.resample(q + "s").mean()
        rr = checkprates_frame.resample(q + "s").mean()
    else:
        r = checkps_frame
        rr = checkprates_frame

    # Plot checkpoints
    plt, fig = plot.setup()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plt.title("Checkpoints")
    plt.plot_date(r.index, r['req'], label="Requested", aa=True,
                  linestyle='solid')
    plt.plot_date(r.index, r['timed'], label="Timed", aa=True,
                  linestyle='solid')
    plt.xlabel("Timestamp", fontweight='semibold')
    plt.ylabel("Checkpoints (since stats reset)", fontweight='semibold')

    fig.axes[0].set_ylim(bottom=0)
    fig.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
    fig.legend()
    fig.tight_layout()
    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_checkp.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)

    # Plot WAL rates
    plt, fig = plot.setup()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plt.title("Checkpoint rate")
    plt.plot_date(rr.index, rr['req'], label="requested",
                  aa=True, linestyle='solid')
    plt.plot_date(rr.index, rr['timed'], label="timed",
                  aa=True, linestyle='solid')
    plt.xlabel("Timestamp", fontweight='semibold')
    plt.ylabel("Avg. checkpoints per minute", fontweight='semibold')
    fig.legend()
    fig.tight_layout()
    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_checkp_rate.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)
    mpclose('all')


# Gather checkpoint data
def calc_checkps(data):
    return {'req': [c['checkpoints_req'] for c in data],
            'timed': [c['checkpoints_timed'] for c in data]}


# Calculate checkpoint rates
def calc_checkprates(data):

    # Checkpoint diff generator - yields tuple list of the rates in
    # checkpoints/minute
    def checkpdiff(data):
        yield (numpy.nan, numpy.nan)
        for i, item in enumerate(data):
            if i + 1 < len(data):
                if data[i + 1]['stats_reset'] == data[i]['stats_reset']:
                    m = (data[i + 1]['snapshot_tstamp']
                         - data[i]['snapshot_tstamp']).total_seconds() / 60
                    yield (round((data[i + 1]['checkpoints_req']
                                  - data[i]['checkpoints_req']) / m, 1),
                           round((data[i + 1]['checkpoints_timed']
                                  - data[i]['checkpoints_timed']) / m, 1))
                else:
                    yield (numpy.nan, numpy.nan)
    rates = list(checkpdiff(data))
    return {'req': [c[0] for c in rates],
            'timed': [c[1] for c in rates]}
