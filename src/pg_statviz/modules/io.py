"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2023 Jimmy Angelakos"
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
     help="date range to be analyzed in ISO 8601 format e.g. 2023-01-01T00:00"
          + " 2023-01-01T23:59")
@arg('-O', '--outputdir', help="output directory")
@arg('--info', help=argparse.SUPPRESS)
@arg('--conn', help=argparse.SUPPRESS)
def io(dbname=getpass.getuser(), host="/var/run/postgresql", port="5432",
       username=getpass.getuser(), password=False, daterange=[],
       outputdir=None, info=None, conn=None):
    "run I/O analysis module"

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

    _logger.info("Running I/O analysis")

    if daterange:
        daterange = [isoparse(d) for d in daterange]
        if daterange[0] > daterange[1]:
            daterange = [daterange[1], daterange[0]]
    else:
        daterange = ['-infinity', 'now()']

    # Retrieve the snapshots from DB
    cur = conn.cursor()
    cur.execute("""SELECT io_stats, block_size, i.stats_reset, snapshot_tstamp
                   FROM pgstatviz.io i
                   JOIN pgstatviz.db USING (snapshot_tstamp)
                   WHERE snapshot_tstamp BETWEEN %s AND %s
                   ORDER BY snapshot_tstamp""",
                (daterange[0], daterange[1]))

    data = cur.fetchmany(MAX_RESULTS)
    if not data:
        raise SystemExit("No pg_statviz snapshots found in this database")

    tstamps = [ts['snapshot_tstamp'] for ts in data]
    blcksz = int(data[0]['block_size'])
    iostats, iokinds = calc_iostats(data, blcksz)

    # Plot as many of each I/O kinds we have per snapshot
    plt, fig, splt1, splt2 = plot.setupdouble()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')

    # Plot Reads
    splt1.set_title("I/O Reads")
    for iokind in iokinds:
        iobytes = []
        for snapshot in iostats:
            found = False
            for entry in snapshot:
                if {'backend_type': entry['backend_type'],
                        'object': entry['object'],
                        'context': entry['context']} == iokind:
                    found = True
                    r = entry['reads']
                    if r:
                        iobytes += round(r / 1073741824, 1 if r >= 100 else 2),
                    else:
                        found = False
            if not found:
                iobytes += 0,
        if not all(b == 0 for b in iobytes):
            splt1.plot_date(tstamps, iobytes,
                            label=f"{iokind['object']}/"
                                  if {iokind['object']} == 'temp relation'
                                  else ""
                                  f"{iokind['backend_type']}/"
                                  f"{iokind['context']}",
                            aa=True, linestyle='solid')
    splt1.set_xlabel("Timestamp", fontweight='semibold')
    splt1.set_ylabel("GB read (at time of snapshot)", fontweight='semibold')
    splt1.set_ylim(bottom=0)
    splt1.legend()

    # Plot Writes
    splt2.set_title("I/O Writes")
    for iokind in iokinds:
        iobytes = []
        for snapshot in iostats:
            found = False
            for entry in snapshot:
                if {'backend_type': entry['backend_type'],
                        'object': entry['object'],
                        'context': entry['context']} == iokind:
                    found = True
                    w = entry['writes']
                    if w:
                        iobytes += round(w / 1073741824, 1 if w >= 100 else 2),
                    else:
                        found = False
            if not found:
                iobytes += 0,
        if not all(b == 0 for b in iobytes):
            splt2.plot_date(tstamps, iobytes,
                            label=f"{iokind['object']}/"
                                  f"{iokind['backend_type']}/"
                                  f"{iokind['context']}",
                            aa=True, linestyle='solid')
    splt2.set_xlabel("Timestamp", fontweight='semibold')
    splt2.set_ylabel("GB written (at time of snapshot)",
                     fontweight='semibold')
    splt2.set_ylim(bottom=0)
    splt2.legend()

    fig.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
    outfile = f"""{outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
        .replace("/", "-")}_{port}_io.png"""
    _logger.info(f"Saving {outfile}")
    plt.gcf().autofmt_xdate()
    fig.tight_layout()
    plt.savefig(outfile)

    iorates = calc_iorates(data, iokinds, blcksz)

    # Plot I/O Rates
    plt, fig, splt1, splt2 = plot.setupdouble()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')

    # Plot Read Rates
    splt1.set_title("I/O Read Rate")
    for iokind in iokinds:
        iokindname = (f"{iokind['object']}/"
                      if {iokind['object']} == 'temp relation'
                      else ""
                      f"{iokind['backend_type']}/"
                      f"{iokind['context']}")
        if not all(numpy.isnan(v) or v == 0
                   for v in iorates['reads'][iokindname]):
            splt1.plot_date(tstamps, [round(v / 1048576, 1 if v >= 100 else 2)
                                      for v in iorates['writes'][iokindname]],
                            label=iokindname, aa=True, linestyle='solid')
    splt1.set_xlabel("Timestamp", fontweight='semibold')
    splt1.set_ylabel("Avg. read rate in MB/s", fontweight='semibold')
    splt1.set_ylim(bottom=0)
    splt1.legend()

    # Plot Write Rates
    splt2.set_title("I/O Write Rate")
    for iokind in iokinds:
        iokindname = (f"{iokind['object']}/"
                      if {iokind['object']} == 'temp relation'
                      else ""
                      f"{iokind['backend_type']}/"
                      f"{iokind['context']}")
        if not all(numpy.isnan(v) or v == 0
                   for v in iorates['writes'][iokindname]):
            splt2.plot_date(tstamps, [round(v / 1048576, 1 if v >= 100 else 2)
                                      for v in iorates['writes'][iokindname]],
                            label=iokindname, aa=True, linestyle='solid')
    splt2.set_xlabel("Timestamp", fontweight='semibold')
    splt2.set_ylabel("Avg. write rate in MB/s", fontweight='semibold')
    splt2.legend()
    splt2.set_ylim(bottom=0)

    plt.gcf().autofmt_xdate()
    fig.tight_layout()
    outfile = f"""{outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
        .replace("/", "-")}_{port}_io_rate.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)


# Gather I/O stats and convert to bytes
def calc_iostats(data, blcksz=8192):
    iostats = [io['io_stats'] for io in data]
    iokinds = []
    for snapshot in iostats:
        for entry in snapshot:
            r = entry['reads']
            if r:
                entry['reads'] = int(r) * blcksz
            w = entry['writes']
            if w:
                entry['writes'] = int(w) * blcksz
            iokind = {'backend_type': entry['backend_type'],
                      'object': entry['object'],
                      'context': entry['context']}
            if iokind not in iokinds:
                iokinds += iokind,
    return iostats, iokinds


# Calculate I/O rates
def calc_iorates(data, iokinds, blcksz=8192):

    rates = {}

    # I/O diff generator - yields tuple list of I/O rates in bytes/s
    def iodiff(data, iokind, rw):
        yield numpy.nan
        for i, item in enumerate(data):
            if i + 1 < len(data):
                if (data[i + 1]['stats_reset'] == data[i]['stats_reset']):
                    s = ((data[i + 1]['snapshot_tstamp']
                         - data[i]['snapshot_tstamp'])
                         .total_seconds())
                    v1, v2 = 0, 0
                    for entry in data[i]['io_stats']:
                        if {'backend_type': entry['backend_type'],
                                'object': entry['object'],
                                'context': entry['context']} == iokind:
                            v1 = entry[rw]
                    for entry in data[i + 1]['io_stats']:
                        if {'backend_type': entry['backend_type'],
                                'object': entry['object'],
                                'context': entry['context']} == iokind:
                            v2 = entry[rw]
                    yield (v2 - v1) / s if v1 else numpy.nan
                else:
                    yield numpy.nan

    for rw in ['reads', 'writes']:
        rates[rw] = {}
        for iokind in iokinds:
            rates[rw][f"{iokind['object']}/"
                      if {iokind['object']} == 'temp relation'
                      else ""
                      f"{iokind['backend_type']}/"
                      f"{iokind['context']}"] = list(iodiff(data, iokind, rw))
    return rates
