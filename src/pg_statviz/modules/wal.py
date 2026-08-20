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
from pg_statviz.libs.ai import (AI_HELP, AI_PROVIDERS,
                                DEFAULT_AI_PROVIDER,
                                run_chart_analysis)
from pg_statviz.libs.dbconn import dbconn
from pg_statviz.libs.html_report import finalize_module_report
from pg_statviz.libs.info import getinfo, get_settings


@arg('-d', '--dbname', help="database name to analyze")
@arg('-h', '--host', metavar="HOSTNAME",
     help="database server host or socket directory")
@arg('-p', '--port', help="database server port")
@arg('-U', '--username', help="database user name")
@arg('-W', '--password', action='store_true',
     help="force password prompt (should happen automatically)")
@arg('-D', '--daterange', nargs=2, metavar=('FROM', 'TO'), type=str,
     help="date range to be analyzed in ISO 8601 format e.g. 2026-01-01T00:00 "
          + "2026-01-01T23:59")
@arg('-O', '--outputdir', help="output directory")
@arg('--ai', nargs='?', const=DEFAULT_AI_PROVIDER, default=None,
     choices=AI_PROVIDERS, metavar='PROVIDER',
     help=AI_HELP)
@arg('--info', help=argparse.SUPPRESS)
@arg('--conn', help=argparse.SUPPRESS)
def wal(*, dbname=getpass.getuser(), host="/var/run/postgresql", port="5432",
        username=getpass.getuser(), password=None, daterange=[],
        outputdir=None, ai=None, info=None, conn=None):
    "run WAL generation analysis module"

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

    _logger.info("Running WAL generation analysis")

    if daterange:
        daterange = [isoparse(d) for d in daterange]
        if daterange[0] > daterange[1]:
            daterange = [daterange[1], daterange[0]]
    else:
        daterange = ['-infinity', 'now()']

    # Retrieve the snapshots from DB
    cur = conn.cursor()
    cur.execute("""SELECT wal_bytes, snapshot_tstamp, stats_reset
                   FROM pgstatviz.wal
                   WHERE snapshot_tstamp BETWEEN %s AND %s
                   ORDER BY snapshot_tstamp""",
                (daterange[0], daterange[1]))
    data = cur.fetchall()
    if not data:
        cur.execute("""SELECT
                    (current_setting('server_version_num')::int >= 140000)
                    AS version_ok""")
        versioncheck = cur.fetchone()['version_ok']
        if not versioncheck:
            _logger.warning("WAL generation analysis is only available from "
                            + "PostgreSQL release 14 onwards")
            return
        else:
            raise SystemExit("No pg_statviz snapshots found in this database")

    tstamps = [t['snapshot_tstamp'] for t in data]
    walgb = calc_wal(data)
    walrates = calc_walrates(data)
    settings = get_settings(conn, ['max_wal_size', 'max_wal_senders',
                                   'max_replication_slots'])

    # Downsample if needed
    walgb_frame = DataFrame(data=walgb, index=tstamps, copy=False)
    walrates_frame = DataFrame(data=walrates, index=tstamps, copy=False)
    if len(tstamps) > plot.MAX_POINTS:
        q = str(round(
            (tstamps[-1] - tstamps[0]).total_seconds() / plot.MAX_POINTS, 2))
        r = walgb_frame.resample(q + "s").mean()
        rr = walrates_frame.resample(q + "s").mean()
    else:
        r = walgb_frame
        rr = walrates_frame

    report_sections = []

    # Plot WAL in GB
    plt, fig = plot.setup()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plt.title("WAL generated")
    plt.plot(r.index, r, label="WAL")
    plt.xlabel("Timestamp", fontweight='semibold')
    plt.ylabel("GB generated (since stats reset)", fontweight='semibold')
    fig.axes[0].set_ylim(bottom=0)
    fig.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
    fig.legend()
    fig.tight_layout()
    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_wal.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)
    run_chart_analysis(
        report_sections, ai, r, "WAL Generated",
        metric_description="CUMULATIVE COUNTER — rising values are "
                           "NORMAL. Large WAL = many writes or "
                           "full_page_writes after checkpoint. "
                           "Affects replication lag, backup size, "
                           "and recovery time. No warning threshold. "
                           "Default to [HEALTHY].",
        outfile=outfile,
        info=info,
        settings=settings,
    )

    # Plot WAL rates
    plt, fig = plot.setup()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plt.title("WAL generation rate")
    plt.plot(rr.index, rr, label="WAL")
    plt.xlabel("Timestamp", fontweight='semibold')
    plt.ylabel("Avg. WAL generation rate (MB/s)", fontweight='semibold')
    fig.legend()
    fig.tight_layout()
    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_wal_rate.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)
    run_chart_analysis(
        report_sections, ai, rr, "WAL Generation Rate",
        metric_description="WAL generation RATE (derived from "
                           "cumulative counter). NaN values appear "
                           "on stats_reset — IGNORE them. High rates "
                           "increase replication lag and I/O "
                           "pressure. Spikes after checkpoint due to "
                           "full_page_writes are normal. Warn only "
                           "if sustained mean >100 MB/s. Default to "
                           "[HEALTHY].",
        outfile=outfile,
        info=info,
        settings=settings,
    )

    finalize_module_report(outputdir, info, port, 'wal',
                           report_sections)
    mpclose('all')


# Gather WAL data & convert to GB
def calc_wal(data):
    return [round(w['wal_bytes'] / 1073741824, 1) for w in data]


# Calculate WAL rates
def calc_walrates(data):

    # WAL diff generator - yields list of the rates in MB/s
    def waldiff(data):
        yield numpy.nan
        for i, item in enumerate(data):
            if i + 1 < len(data):
                if data[i + 1]['stats_reset'] == data[i]['stats_reset']:
                    s = (data[i + 1]['snapshot_tstamp']
                         - data[i]['snapshot_tstamp']).total_seconds()
                    yield (int(data[i + 1]['wal_bytes'])
                           - int(data[i]['wal_bytes'])) / 1048576 / s
                else:
                    yield numpy.nan
    rates = list(waldiff(data))
    return [round(w, 1 if w >= 100 else 2) for w in rates]
