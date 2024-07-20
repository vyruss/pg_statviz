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
     help="date range to be analyzed in ISO 8601 format e.g. 2023-01-01T00:00 "
          + "2023-01-01T23:59")
@arg('-O', '--outputdir', help="output directory")
@arg('--info', help=argparse.SUPPRESS)
@arg('--conn', help=argparse.SUPPRESS)
def buf(*, dbname=getpass.getuser(), host="/var/run/postgresql", port="5432",
        username=getpass.getuser(), password=False, daterange=[],
        outputdir=None, info=None, conn=None):
    "run buffers written analysis module"

    MAX_RESULTS = 100000

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

    _logger.info("Running buffers written analysis")

    if daterange:
        daterange = [isoparse(d) for d in daterange]
        if daterange[0] > daterange[1]:
            daterange = [daterange[1], daterange[0]]
    else:
        daterange = ['-infinity', 'now()']

    # Retrieve the snapshots from DB
    cur = conn.cursor()
    cur.execute("""SELECT buffers_checkpoint, buffers_clean, buffers_backend,
                          b.stats_reset, snapshot_tstamp, block_size
                   FROM pgstatviz.buf b
                   JOIN pgstatviz.db USING (snapshot_tstamp)
                   WHERE snapshot_tstamp BETWEEN %s AND %s
                   ORDER BY snapshot_tstamp""",
                (daterange[0], daterange[1]))
    data = cur.fetchmany(MAX_RESULTS)
    if not data:
        raise SystemExit("No pg_statviz snapshots found in this database")

    tstamps = [t['snapshot_tstamp'] for t in data]
    blcksz = int(data[0]['block_size'])

    buffers = calc_buffers(data, blcksz)

    # Plot buffers
    plt, fig = plot.setup()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plt.title("Buffers written")
    plt.plot_date(tstamps, buffers['total'], label="total", aa=True,
                  linestyle='solid')
    plt.plot_date(tstamps, buffers['checkpoints'], label="checkpoints",
                  aa=True, linestyle='solid')
    plt.plot_date(tstamps, buffers['bgwriter'], label="bgwriter", aa=True,
                  linestyle='solid')
    plt.plot_date(tstamps, buffers['backends'], label="backends", aa=True,
                  linestyle='solid')
    plt.xlabel("Timestamp", fontweight='semibold')
    plt.ylabel("GB written (since stats reset)", fontweight='semibold')
    fig.axes[0].set_ylim(bottom=0)
    fig.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
    fig.legend()
    fig.tight_layout()
    outfile = f"""{outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
        .replace("/", "-")}_{port}_buf.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)

    bufrates = calc_bufrates(data, blcksz)

    # Plot buffer rates
    plt, fig = plot.setup()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plt.title("Buffer write rate")
    plt.plot_date(tstamps, bufrates['total'], label="total", aa=True,
                  linestyle='solid')
    plt.plot_date(tstamps, bufrates['checkpoints'], label="checkpoints",
                  aa=True, linestyle='solid')
    plt.plot_date(tstamps, bufrates['bgwriter'], label="bgwriter", aa=True,
                  linestyle='solid')
    plt.plot_date(tstamps, bufrates['backends'], label="backends", aa=True,
                  linestyle='solid')

    plt.xlabel("Timestamp", fontweight='semibold')
    plt.ylabel("Avg. write rate in MB/s", fontweight='semibold')
    fig.legend()
    fig.tight_layout()
    outfile = f"""{outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
        .replace("/", "-")}_{port}_buf_rate.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)


# Gather buffers and convert to GB
def calc_buffers(data, blcksz=8192):
    bufs = {}
    bufs['total'] = [round((b['buffers_checkpoint']
                            + b['buffers_clean']
                            + b['buffers_backend'])
                           * blcksz / 1073741824, 1) for b in data]
    bufs['checkpoints'] = [round(b['buffers_checkpoint']
                                 * blcksz / 1073741824, 1) for b in data]
    bufs['bgwriter'] = [round(b['buffers_clean']
                              * blcksz / 1073741824, 1) for b in data]
    bufs['backends'] = [round(b['buffers_backend']
                              * blcksz / 1073741824, 1) for b in data]
    return bufs


# Calculate buffer rates
def calc_bufrates(data, blcksz=8192):
    rates = {}

    # Buffer diff generator - yields 3-tuple list of the 3 rates in
    # buffers/s
    def bufdiff(data):
        yield (numpy.nan, numpy.nan, numpy.nan)
        for i, item in enumerate(data):
            if i + 1 < len(data):
                if data[i + 1]['stats_reset'] == data[i]['stats_reset']:
                    s = (data[i + 1]['snapshot_tstamp']
                         - data[i]['snapshot_tstamp']).total_seconds()
                    yield ((data[i + 1]['buffers_checkpoint']
                           - data[i]['buffers_checkpoint']) / s,
                           (data[i + 1]['buffers_clean']
                           - data[i]['buffers_clean']) / s,
                           (data[i + 1]['buffers_backend']
                           - data[i]['buffers_backend']) / s)
                else:
                    yield (numpy.nan, numpy.nan, numpy.nan)

    bufs = list(bufdiff(data))

    # Normalize and round the rate data
    rates['total'] = [round((b[0] + b[1] + b[2]) * blcksz / 1048576,
                            1 if b[0] >= 100 else 2)
                      for b in bufs]
    rates['checkpoints'] = [round(b[0] * blcksz / 1048576,
                                  1 if b[0] >= 100 else 2)
                            for b in bufs]
    rates['bgwriter'] = [round(b[1] * blcksz / 1048576,
                               1 if b[0] >= 100 else 2)
                         for b in bufs]
    rates['backends'] = [round(b[2] * blcksz / 1048576,
                               1 if b[0] >= 100 else 2)
                         for b in bufs]
    return rates
