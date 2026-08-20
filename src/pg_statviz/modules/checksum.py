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
from pg_statviz.libs.ai import (AI_HELP, AI_PROVIDERS,
                                DEFAULT_AI_PROVIDER,
                                run_chart_analysis)
from pg_statviz.libs.dbconn import dbconn
from pg_statviz.libs.html_report import finalize_module_report
from pg_statviz.libs.info import getinfo


@arg('-d', '--dbname', help="database name to analyze")
@arg('-h', '--host', metavar="HOSTNAME",
     help="database server host or socket directory")
@arg('-p', '--port', help="database server port")
@arg('-U', '--username', help="database user name")
@arg('-W', '--password', action='store_true',
     help="force password prompt (should happen automatically)")
@arg('-D', '--daterange', nargs=2, metavar=('FROM', 'TO'), type=str,
     help="date range to be analyzed in ISO 8601 format e.g. "
          + "2026-01-01T00:00 2026-01-01T23:59")
@arg('-O', '--outputdir', help="output directory")
@arg('--ai', nargs='?', const=DEFAULT_AI_PROVIDER, default=None,
     choices=AI_PROVIDERS, metavar='PROVIDER',
     help=AI_HELP)
@arg('--info', help=argparse.SUPPRESS)
@arg('--conn', help=argparse.SUPPRESS)
def checksum(*, dbname=getpass.getuser(), host="/var/run/postgresql",
             port="5432", username=getpass.getuser(), password=None,
             daterange=[], outputdir=None, ai=None, info=None, conn=None):
    "run checksum failure analysis module"

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

    _logger.info("Running checksum failure analysis")

    if daterange:
        daterange = [isoparse(d) for d in daterange]
        if daterange[0] > daterange[1]:
            daterange = [daterange[1], daterange[0]]
    else:
        daterange = ['-infinity', 'now()']

    cur = conn.cursor()
    cur.execute("""SELECT checksum_failures, checksum_last_failure,
                          snapshot_tstamp
                   FROM pgstatviz.db
                   WHERE snapshot_tstamp BETWEEN %s AND %s
                   ORDER BY snapshot_tstamp""",
                (daterange[0], daterange[1]))
    data = cur.fetchall()
    if not data:
        raise SystemExit("No pg_statviz snapshots found in this database")

    tstamps = [t['snapshot_tstamp'] for t in data]
    failures = [t['checksum_failures'] if t['checksum_failures'] is not None
                else 0 for t in data]
    findings = []
    last_failures = failures[-1] if failures else 0
    if last_failures > 0:
        findings.append({
            'severity': 'CRITICAL',
            'message': f'{last_failures} checksum failures recorded - '
                       f'possible data corruption',
        })

    # Downsample if needed
    checksum_frame = DataFrame(
        data={'failures': failures},
        index=tstamps, copy=False)
    if len(tstamps) > plot.MAX_POINTS:
        q = str(round(
            (tstamps[-1] - tstamps[0]).total_seconds() / plot.MAX_POINTS, 2))
        r = checksum_frame.resample(q + "s").max()
    else:
        r = checksum_frame

    report_sections = []

    # Plot checksum failures
    plt, fig = plot.setup()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plt.title("Checksum failures")

    plt.plot(r.index, r['failures'], label="Checksum failures")
    plt.xlabel("Timestamp", fontweight='semibold')
    plt.ylabel("Cumulative checksum failures", fontweight='semibold')
    fig.axes[0].set_ylim(bottom=0)
    fig.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
    fig.legend()
    fig.tight_layout()
    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_checksum.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)
    run_chart_analysis(
        report_sections, ai, r, "Checksum Failures",
        metric_description="Data page checksum failures. ANY non-zero value "
                           "indicates DATA CORRUPTION - this is CRITICAL. "
                           "Causes: bad RAM, failing storage, filesystem "
                           "bugs, or incomplete writes. Investigate "
                           "immediately with pg_verify_checksums.",
        outfile=outfile,
        info=info,
        findings=findings,
    )

    finalize_module_report(outputdir, info, port, 'checksum',
                           report_sections)
    mpclose('all')
