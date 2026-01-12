"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2026 Jimmy Angelakos"
__license__ = "PostgreSQL License"

import argparse
import getpass
import logging
from argh.decorators import arg
from dateutil.parser import isoparse
from matplotlib.pyplot import close as mpclose
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
     help="date range to be analyzed in ISO 8601 format e.g. 2026-01-01T00:00"
          + " 2026-01-01T23:59")
@arg('-O', '--outputdir', help="output directory")
@arg('--info', help=argparse.SUPPRESS)
@arg('--conn', help=argparse.SUPPRESS)
def slru(*, dbname=getpass.getuser(), host="/var/run/postgresql", port="5432",
         username=getpass.getuser(), password=False, daterange=[],
         outputdir=None, info=None, conn=None):
    "run SLRU analysis module"

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

    _logger.info("Running SLRU analysis")

    if daterange:
        daterange = [isoparse(d) for d in daterange]
        if daterange[0] > daterange[1]:
            daterange = [daterange[1], daterange[0]]
    else:
        daterange = ['-infinity', 'now()']

    # Retrieve the snapshots from DB
    cur = conn.cursor()
    cur.execute("""SELECT slru_stats, snapshot_tstamp
                   FROM pgstatviz.slru
                   WHERE snapshot_tstamp BETWEEN %s AND %s
                   ORDER BY snapshot_tstamp""",
                (daterange[0], daterange[1]))
    data = cur.fetchall()
    if not data:
        raise SystemExit("No pg_statviz snapshots found in this database")

    tstamps = [t['snapshot_tstamp'] for t in data]
    slru_stats = [s['slru_stats'] for s in data]

    # Determine all SLRU names
    slru_names = []
    for ss in slru_stats:
        if ss:
            for s in ss:
                if s['name'] not in slru_names:
                    slru_names += s['name'],

    # Plot SLRU hit ratios and read rates
    plt, fig, splt1, splt2 = plot.setupdouble()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')

    # Plot SLRU hit ratios
    splt1.set_title("SLRU cache hit ratio")
    for name in slru_names:
        hit_ratios = []
        for ss in slru_stats:
            if not ss:
                hit_ratios += 0,
            else:
                found = False
                for s in ss:
                    if s['name'] == name:
                        total = s['blks_hit'] + s['blks_read']
                        if total > 0:
                            hit_ratios += (s['blks_hit'] / total) * 100,
                        else:
                            hit_ratios += 0,
                        found = True
                if not found:
                    hit_ratios += 0,
        if not all(c == 0 for c in hit_ratios):
            # Downsample if needed
            hr_frame = DataFrame(data={name: hit_ratios}, index=tstamps,
                                 copy=False)
            if len(tstamps) > plot.MAX_POINTS:
                q = str(round(
                    (tstamps[-1] - tstamps[0]).total_seconds()
                    / plot.MAX_POINTS, 2))
                r = hr_frame.resample(q + "s").mean()
            else:
                r = hr_frame
            splt1.plot_date(r.index, r[name], label=name, aa=True,
                            linestyle='solid')
    splt1.set_xlabel("Timestamp", fontweight='semibold')
    splt1.set_ylabel("Hit ratio (%)", fontweight='semibold')
    splt1.set_ylim(0, 100)
    splt1.legend()

    # Plot SLRU reads
    splt2.set_title("SLRU block reads")
    for name in slru_names:
        reads = []
        for ss in slru_stats:
            if not ss:
                reads += 0,
            else:
                found = False
                for s in ss:
                    if s['name'] == name:
                        reads += s['blks_read'],
                        found = True
                if not found:
                    reads += 0,
        if not all(c == 0 for c in reads):
            # Downsample if needed
            read_frame = DataFrame(data={name: reads}, index=tstamps,
                                   copy=False)
            if len(tstamps) > plot.MAX_POINTS:
                q = str(round(
                    (tstamps[-1] - tstamps[0]).total_seconds()
                    / plot.MAX_POINTS, 2))
                r = read_frame.resample(q + "s").sum()
            else:
                r = read_frame
            splt2.plot_date(r.index, r[name], label=name, aa=True,
                            linestyle='solid')
    splt2.set_xlabel("Timestamp", fontweight='semibold')
    splt2.set_ylabel("Blocks read", fontweight='semibold')
    splt2.set_ylim(bottom=0)
    splt2.legend()

    plt.gcf().autofmt_xdate()
    fig.tight_layout()
    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_slru.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)
    mpclose('all')
