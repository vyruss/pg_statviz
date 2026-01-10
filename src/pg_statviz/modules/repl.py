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
def repl(*, dbname=getpass.getuser(), host="/var/run/postgresql", port="5432",
         username=getpass.getuser(), password=False, daterange=[],
         outputdir=None, info=None, conn=None):
    "run replication analysis module"

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

    _logger.info("Running replication analysis")

    if daterange:
        daterange = [isoparse(d) for d in daterange]
        if daterange[0] > daterange[1]:
            daterange = [daterange[1], daterange[0]]
    else:
        daterange = ['-infinity', 'now()']

    # Retrieve the snapshots from DB
    cur = conn.cursor()
    cur.execute("""SELECT standby_lag, slot_stats, snapshot_tstamp
                   FROM pgstatviz.repl
                   WHERE snapshot_tstamp BETWEEN %s AND %s
                   ORDER BY snapshot_tstamp""",
                (daterange[0], daterange[1]))
    data = cur.fetchall()
    if not data:
        raise SystemExit("No pg_statviz snapshots found in this database")

    tstamps = [t['snapshot_tstamp'] for t in data]
    standby_lag = [s['standby_lag'] for s in data]
    slot_stats = [s['slot_stats'] for s in data]

    # Plot standby lag and slot WAL retention
    plt, fig, splt1, splt2 = plot.setupdouble()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')

    # Plot Standby lag
    splt1.set_title("Standby replication lag")
    # Determine all standbys
    standbys = []
    for sl in standby_lag:
        if sl:
            for s in sl:
                if s['application_name'] not in standbys:
                    standbys += s['application_name'],
    # Plot lag for each standby
    for sb in standbys:
        lag_bytes = []
        for sl in standby_lag:
            if not sl:
                lag_bytes += 0,
            else:
                found = False
                for s in sl:
                    if s['application_name'] == sb:
                        lag_bytes += (s['lag_bytes']
                                      if s['lag_bytes'] is not None
                                      else 0),
                        found = True
                if not found:
                    lag_bytes += 0,
        if not all(c == 0 for c in lag_bytes):
            # Downsample if needed
            lag_frame = DataFrame(data={sb: lag_bytes}, index=tstamps,
                                  copy=False)
            if len(tstamps) > plot.MAX_POINTS:
                q = str(round(
                    (tstamps[-1] - tstamps[0]).total_seconds()
                    / plot.MAX_POINTS, 2))
                r = lag_frame.resample(q + "s").max()
            else:
                r = lag_frame
            splt1.plot_date(r.index, r[sb], label=sb, aa=True,
                            linestyle='solid')
    splt1.set_xlabel("Timestamp", fontweight='semibold')
    splt1.set_ylabel("Lag (bytes)", fontweight='semibold')
    splt1.set_ylim(bottom=0)
    splt1.legend()

    # Plot Slot WAL accumulation
    splt2.set_title("Replication slot WAL retention")
    # Determine all slots
    slots = []
    for ss in slot_stats:
        if ss:
            for s in ss:
                if s['slot_name'] not in slots:
                    slots += s['slot_name'],
    # Plot WAL bytes for each slot
    for slot in slots:
        wal_bytes = []
        for ss in slot_stats:
            if not ss:
                wal_bytes += 0,
            else:
                found = False
                for s in ss:
                    if s['slot_name'] == slot:
                        wal_bytes += (s['wal_bytes']
                                      if s['wal_bytes'] is not None
                                      else 0),
                        found = True
                if not found:
                    wal_bytes += 0,
        if not all(c == 0 for c in wal_bytes):
            # Downsample if needed
            wal_frame = DataFrame(data={slot: wal_bytes}, index=tstamps,
                                  copy=False)
            if len(tstamps) > plot.MAX_POINTS:
                q = str(round(
                    (tstamps[-1] - tstamps[0]).total_seconds()
                    / plot.MAX_POINTS, 2))
                r = wal_frame.resample(q + "s").max()
            else:
                r = wal_frame
            splt2.plot_date(r.index, r[slot], label=slot, aa=True,
                            linestyle='solid')
    splt2.set_xlabel("Timestamp", fontweight='semibold')
    splt2.set_ylabel("WAL retention (bytes)", fontweight='semibold')
    splt2.set_ylim(bottom=0)
    splt2.legend()

    plt.gcf().autofmt_xdate()
    fig.tight_layout()
    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_repl.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)
    mpclose('all')
