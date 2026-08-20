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
from pg_statviz.libs.info import getinfo, get_settings


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
@arg('--ai', nargs='?', const=DEFAULT_AI_PROVIDER, default=None,
     choices=AI_PROVIDERS, metavar='PROVIDER',
     help=AI_HELP)
@arg('--info', help=argparse.SUPPRESS)
@arg('--conn', help=argparse.SUPPRESS)
def blocking(*, dbname=getpass.getuser(), host="/var/run/postgresql",
             port="5432", username=getpass.getuser(), password=None,
             daterange=[], outputdir=None, ai=None, info=None, conn=None):
    "run blocking locks analysis module"

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

    _logger.info("Running blocking locks analysis")

    if daterange:
        daterange = [isoparse(d) for d in daterange]
        if daterange[0] > daterange[1]:
            daterange = [daterange[1], daterange[0]]
    else:
        daterange = ['-infinity', 'now()']

    # Retrieve the snapshots from DB
    cur = conn.cursor()
    cur.execute("""SELECT blocked_total, blockers_total, blocking,
                          snapshot_tstamp
                   FROM pgstatviz.blocking
                   WHERE snapshot_tstamp BETWEEN %s AND %s
                   ORDER BY snapshot_tstamp""",
                (daterange[0], daterange[1]))
    data = cur.fetchall()
    if not data:
        raise SystemExit("No pg_statviz snapshots found in this database")

    tstamps = [ts['snapshot_tstamp'] for ts in data]
    blocked = [b['blocked_total'] or 0 for b in data]
    blockers = [b['blockers_total'] or 0 for b in data]
    details = [d['blocking'] for d in data]
    settings = get_settings(conn, ['deadlock_timeout',
                                   'lock_timeout',
                                   'idle_in_transaction_session_timeout',
                                   'max_locks_per_transaction'])

    report_sections = []

    # Plot blocked and blocking session counts
    counts_frame = DataFrame(data={'Blocked sessions': blocked,
                                   'Blocking sessions': blockers},
                             index=tstamps, copy=False)
    # Downsample if needed
    if len(tstamps) > plot.MAX_POINTS:
        q = str(round(
            (tstamps[-1] - tstamps[0]).total_seconds() / plot.MAX_POINTS, 2))
        r = counts_frame.resample(q + "s").mean()
    else:
        r = counts_frame

    plt, fig = plot.setup()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plt.title("Blocking locks")
    plt.plot(r.index, r['Blocked sessions'], label="Blocked sessions")
    plt.plot(r.index, r['Blocking sessions'], label="Blocking sessions")
    fig.axes[0].set_ylim(bottom=0)
    fig.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
    plt.xlabel("Timestamp", fontweight='semibold')
    plt.ylabel("Session count (at time of snapshot)", fontweight='semibold')
    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_blocking.png"""
    _logger.info(f"Saving {outfile}")
    fig.legend()
    fig.tight_layout()
    plt.savefig(outfile)
    run_chart_analysis(
        report_sections, ai, r, "Blocking Locks",
        metric_description="POINT-IN-TIME session counts caught waiting on "
                           "a lock at each snapshot. Zero across the board is "
                           "the healthy case and needs no comment. Any "
                           "sustained non-zero blocked count means queries "
                           "are serialising behind a lock holder; look at "
                           "long-running transactions, missing indexes "
                           "forcing wide row locks, and DDL taking "
                           "AccessExclusive. Snapshot sampling means brief "
                           "blocking can be missed entirely, so treat counts "
                           "as a lower bound.",
        outfile=outfile,
        info=info,
        settings=settings,
        findings=calc_findings(blocked, blockers),
    )

    # Plot the breakdown by lock type, when there is any blocking at all
    locktypes = find_locktypes(details)
    if locktypes:
        plt, fig = plot.setup()
        plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                     fontweight='semibold')
        plt.title("Blocking locks by type")
        types_frame = DataFrame(
            data={lt: count_by_locktype(details, lt) for lt in locktypes},
            index=tstamps, copy=False)
        if len(tstamps) > plot.MAX_POINTS:
            q = str(round(
                (tstamps[-1] - tstamps[0]).total_seconds()
                / plot.MAX_POINTS, 2))
            rr = types_frame.resample(q + "s").mean()
        else:
            rr = types_frame
        for lt in locktypes:
            if not all(c == 0 for c in rr[lt]):
                plt.plot(rr.index, rr[lt], label=lt)
        fig.axes[0].set_ylim(bottom=0)
        fig.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
        plt.xlabel("Timestamp", fontweight='semibold')
        plt.ylabel("Blocking events by type", fontweight='semibold')
        outfile = f"""{
            outputdir.rstrip("/") + "/" if outputdir
            else ''}pg_statviz_{info['hostname'].replace("/", "-")
                                }_{port}_blocking_types.png"""
        _logger.info(f"Saving {outfile}")
        fig.legend()
        fig.tight_layout()
        plt.savefig(outfile)
        run_chart_analysis(
            report_sections, ai, rr, "Blocking Locks by Type",
            metric_description="Blocking events split by pg_locks.locktype. "
                               "'transactionid' dominating means sessions "
                               "wait on each other's transactions to commit; "
                               "'relation' points at DDL or explicit LOCK "
                               "TABLE; 'tuple' means row-level contention on "
                               "the same rows. Use the mix to decide where to "
                               "look, not as a severity signal on its own.",
            outfile=outfile,
            info=info,
            settings=settings,
        )

    finalize_module_report(outputdir, info, port, 'blocking',
                           report_sections)
    mpclose('all')


# Distinct lock types across all snapshots, in first-seen order
def find_locktypes(details):
    locktypes = []
    for snapshot in details or []:
        for entry in snapshot or []:
            lt = entry.get('lock_type')
            if lt is not None and lt not in locktypes:
                locktypes += lt,
    return locktypes


# Per-snapshot blocked-session count for one lock type
def count_by_locktype(details, locktype):
    return [sum(entry.get('blocked_count') or 0
                for entry in snapshot or []
                if entry.get('lock_type') == locktype)
            for snapshot in details or []]


# Deterministic rule findings handed to the LLM and used as a severity floor
def calc_findings(blocked, blockers):
    findings = []
    if not blocked:
        return findings
    peak = max(blocked)
    if peak == 0:
        return findings
    affected = sum(1 for b in blocked if b > 0)
    findings.append({
        # Blocking in more than half the snapshots is contention, not a blip
        'severity': ('CRITICAL' if affected * 2 > len(blocked)
                     else 'WARNING'),
        'message': f'{peak} session(s) blocked at peak by '
                   f'{max(blockers) if blockers else 0} blocker(s), '
                   f'in {affected} of {len(blocked)} snapshots',
    })
    return findings
