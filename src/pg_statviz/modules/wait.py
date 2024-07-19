"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2024 Jimmy Angelakos"
__license__ = "PostgreSQL License"

import argparse
import getpass
import logging
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
def wait(dbname=getpass.getuser(), host="/var/run/postgresql", port="5432",
         username=getpass.getuser(), password=False, daterange=[],
         outputdir=None, info=None, conn=None):
    "run wait events analysis module"

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

    _logger.info("Running wait events analysis")

    if daterange:
        daterange = [isoparse(d) for d in daterange]
        if daterange[0] > daterange[1]:
            daterange = [daterange[1], daterange[0]]
    else:
        daterange = ['-infinity', 'now()']

    # Retrieve the snapshots from DB
    cur = conn.cursor()
    cur.execute("""SELECT wait_events_total, wait_events, snapshot_tstamp
                   FROM pgstatviz.wait
                   WHERE snapshot_tstamp BETWEEN %s AND %s
                   ORDER BY snapshot_tstamp""",
                (daterange[0], daterange[1]))
    data = cur.fetchmany(MAX_RESULTS)
    if not data:
        raise SystemExit("No pg_statviz snapshots found in this database")

    tstamps = [t['snapshot_tstamp'] for t in data]
    wevents = [w['wait_events'] for w in data]
    total = [t['wait_events_total'] for t in data]

    # Determine all kinds of wait event for plotting
    waitkinds = []
    for w in wevents:
        for e in w:
            if 'wait_event' in e:
                wk = {'wait_event_type': e['wait_event_type'],
                      'wait_event': e['wait_event']}
                if wk not in waitkinds:
                    waitkinds += wk,

    # Plot as many of each wait event kind we have per snapshot
    plt, fig = plot.setup()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plt.title("Wait events")
    for wk in waitkinds:
        wc = []
        for w in wevents:
            if not w:
                wc += 0,
            else:
                found = False
                for e in w:
                    if wk.items() <= e.items():
                        wc += e['wait_event_count'],
                        found = True
                if not found:
                    wc += 0,
        if not all(c == 0 for c in wc):
            plt.plot_date(tstamps, wc,
                          label=f"{wk['wait_event_type']}/{wk['wait_event']}",
                          aa=True, linestyle='solid')
    # Plot total wait events
    plt.plot_date(tstamps, total,
                  label='Total', aa=True, linestyle='solid')
    fig.axes[0].set_ylim(bottom=0)
    fig.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
    plt.xlabel("Timestamp", fontweight='semibold')
    plt.ylabel("Wait event count (at time of snapshot)", fontweight='semibold')
    outfile = f"""{outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
        .replace("/", "-")}_{port}_wait.png"""
    _logger.info(f"Saving {outfile}")
    fig.legend()
    fig.tight_layout()
    plt.savefig(outfile)
