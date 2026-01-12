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
     help="date range to be analyzed in ISO 8601 format e.g. 2026-01-01T00:00"
          + " 2026-01-01T23:59")
@arg('-O', '--outputdir', help="output directory")
@arg('--info', help=argparse.SUPPRESS)
@arg('--conn', help=argparse.SUPPRESS)
def lock(*, dbname=getpass.getuser(), host="/var/run/postgresql", port="5432",
         username=getpass.getuser(), password=False, daterange=[],
         outputdir=None, info=None, conn=None):
    "run locks analysis module"

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

    _logger.info("Running locks analysis")

    if daterange:
        daterange = [isoparse(d) for d in daterange]
        if daterange[0] > daterange[1]:
            daterange = [daterange[1], daterange[0]]
    else:
        daterange = ['-infinity', 'now()']

    # Retrieve the snapshots from DB
    cur = conn.cursor()
    cur.execute("""SELECT locks_total, locks, snapshot_tstamp
                   FROM pgstatviz.lock
                   WHERE snapshot_tstamp BETWEEN %s AND %s
                   ORDER BY snapshot_tstamp""",
                (daterange[0], daterange[1]))
    data = cur.fetchall()
    if not data:
        raise SystemExit("No pg_statviz snapshots found in this database")

    tstamps = [ts['snapshot_tstamp'] for ts in data]
    locks = [lo['locks'] for lo in data]
    total = [tl['locks_total'] for tl in data]

    # Determine all lock modes for plotting
    lockmodes = []
    for snapshot in locks:
        for entry in snapshot:
            if 'lock_mode' in entry:
                lm = entry['lock_mode']
                if lm not in lockmodes:
                    lockmodes += lm,

    # Plot as many of each lock mode we have per snapshot
    plt, fig = plot.setup()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plt.title("Locks")
    for lm in lockmodes:
        lc = []
        for lo in locks:
            found = False
            for c in lo:
                if c['lock_mode'] == lm:
                    found = True
                    lc += c['lock_count'],
            if not found:
                lc += 0,
        lc_frame = DataFrame(data={lm: lc}, index=tstamps, copy=False)
        # Downsample if needed
        if len(tstamps) > plot.MAX_POINTS:
            q = str(round(
                (tstamps[-1] - tstamps[0]).total_seconds()
                / plot.MAX_POINTS, 2))
            r = lc_frame.resample(q + "s").mean()
        else:
            r = lc_frame
        if not all(c == 0 for c in r[lm]):
            plt.plot_date(r.index, r[lm],
                          label=lm, aa=True, linestyle='solid')

    # Plot total locks
    # # Downsample if needed
    total_frame = DataFrame(data=total, index=tstamps, copy=False)
    if len(tstamps) > plot.MAX_POINTS:
        q = str(round(
            (tstamps[-1] - tstamps[0]).total_seconds()
            / plot.MAX_POINTS, 2))
        rr = total_frame.resample(q + "s").mean()
    else:
        rr = total_frame
    plt.plot_date(rr.index, rr, label='Total', aa=True, linestyle='solid')
    fig.axes[0].set_ylim(bottom=0)
    fig.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
    plt.xlabel("Timestamp", fontweight='semibold')
    plt.ylabel("Lock count (at time of snapshot)", fontweight='semibold')
    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_lock.png"""
    _logger.info(f"Saving {outfile}")
    fig.legend()
    fig.tight_layout()
    plt.savefig(outfile)
    mpclose('all')
