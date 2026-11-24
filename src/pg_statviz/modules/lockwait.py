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
from numpy import nan
from pandas import DataFrame, Series, concat
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
def lockwait(*, dbname=getpass.getuser(), host="/var/run/postgresql",
             port="5432", username=getpass.getuser(), password=None,
             daterange=[], outputdir=None, ai=None, info=None, conn=None):
    "run lock wait statistics analysis module"

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

    _logger.info("Running lock wait statistics analysis")

    if daterange:
        daterange = [isoparse(d) for d in daterange]
        if daterange[0] > daterange[1]:
            daterange = [daterange[1], daterange[0]]
    else:
        daterange = ['-infinity', 'now()']

    # Retrieve the snapshots from DB
    cur = conn.cursor()
    cur.execute("""SELECT lockwait_stats, stats_reset, snapshot_tstamp
                   FROM pgstatviz.lockwait
                   WHERE snapshot_tstamp BETWEEN %s AND %s
                   ORDER BY snapshot_tstamp""",
                (daterange[0], daterange[1]))
    data = cur.fetchall()
    if not data:
        cur.execute("""SELECT
                    (current_setting('server_version_num')::int >= 190000)
                    AS version_ok""")
        if not cur.fetchone()['version_ok']:
            _logger.warning("Lock wait statistics are only available from "
                            + "PostgreSQL release 19 onwards")
            return
        raise SystemExit("No pg_statviz snapshots found in this database")

    waits = calc_deltas(data, 'waits')
    wait_time = calc_deltas(data, 'wait_time')
    fastpath = calc_deltas(data, 'fastpath_exceeded')
    minutes = calc_minutes(data)
    findings = calc_findings(waits, wait_time)
    settings = get_settings(conn, ['deadlock_timeout', 'log_lock_waits',
                                   'max_locks_per_transaction'])

    # Downsample if needed, summing counts and minutes so that rates and
    # average waits stay exact
    tstamps = minutes.index
    if len(tstamps) > plot.MAX_POINTS:
        q = str(round((tstamps[-1] - tstamps[0]).total_seconds()
                      / plot.MAX_POINTS, 2)) + "s"
        waits, wait_time, fastpath, minutes = (
            f.resample(q).sum(min_count=1)
            for f in (waits, wait_time, fastpath, minutes))
    avg_wait = calc_avg_wait(waits, wait_time)
    wait_rate = waits.div(minutes, axis=0)
    fastpath_rate = fastpath.div(minutes, axis=0)
    total = wait_rate.sum(axis=1).where(minutes.notna())
    waited = [lt for lt in wait_rate.columns if wait_rate[lt].fillna(0).any()]

    report_sections = []

    # Plot lock wait rate and average wait per lock type
    plt, fig, splt1, splt2 = plot.setupdouble()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    splt2.sharex(splt1)
    splt1.set_title("Lock waits longer than deadlock_timeout")
    splt2.set_title("Average lock wait")
    for lt in waited:
        splt1.plot(wait_rate.index, wait_rate[lt], label=lt)
        splt2.plot(avg_wait.index, avg_wait[lt], label=lt)
    splt1.plot(total.index, total, label="Total")
    splt1.set_ylabel("Waits per minute", fontweight='semibold')
    splt2.set_ylabel("Milliseconds per wait", fontweight='semibold')
    for s in (splt1, splt2):
        s.set_xlabel("Timestamp", fontweight='semibold')
        s.set_ylim(bottom=0)
    # One legend for both panels, which plot each lock type in the same color
    fig.legend(handles=splt1.get_lines())
    plt.gcf().autofmt_xdate()
    fig.tight_layout()
    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_lockwait.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)
    run_chart_analysis(
        report_sections, ai,
        concat([total.rename("Total waits/min"),
                wait_rate[waited].add_suffix(" waits/min"),
                avg_wait[waited].add_suffix(" ms per wait")], axis=1),
        "Lock Waits",
        metric_description="Lock waits per minute and average wait in "
                           "milliseconds, by lock type, from pg_stat_lock "
                           "(whole cluster). Rates are differences between "
                           "snapshots, so they include every wait between "
                           "snapshots, not only ones a snapshot caught. Only "
                           "waits that lasted longer than deadlock_timeout "
                           "and then acquired the lock are counted. A Total "
                           "of zero is the healthy case and needs no "
                           "comment. Any non-zero rate is a WARNING: "
                           "sessions queue longer than deadlock_timeout for "
                           "locks. Long averages point at long-running "
                           "transactions holding locks; 'transactionid' and "
                           "'tuple' mean row-level contention, 'relation' "
                           "means DDL or LOCK TABLE. log_lock_waits logs "
                           "each such wait with the lock holders and the "
                           "wait queue.",
        outfile=outfile,
        info=info,
        settings=settings,
        findings=findings,
    )

    # Plot fast-path lock slot overflows, when there are any
    overflowed = [lt for lt in fastpath_rate.columns
                  if fastpath_rate[lt].fillna(0).any()]
    if overflowed:
        plt, fig = plot.setup()
        plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                     fontweight='semibold')
        plt.title("Fast-path lock slot overflows")
        for lt in overflowed:
            plt.plot(fastpath_rate.index, fastpath_rate[lt], label=lt)
        fig.axes[0].set_ylim(bottom=0)
        plt.xlabel("Timestamp", fontweight='semibold')
        plt.ylabel("Overflows per minute", fontweight='semibold')
        outfile = f"""{
            outputdir.rstrip("/") + "/" if outputdir
            else ''}pg_statviz_{info['hostname'].replace("/", "-")
                                }_{port}_lockwait_fastpath.png"""
        _logger.info(f"Saving {outfile}")
        fig.legend()
        fig.tight_layout()
        plt.savefig(outfile)
        run_chart_analysis(
            report_sections, ai,
            fastpath_rate[overflowed].add_suffix(" overflows/min"),
            "Fast-path Lock Slot Overflows",
            metric_description="Relation lock requests per minute that "
                               "could not use a fast-path lock slot because "
                               "the backend's slots were full, so they went "
                               "through the shared lock table. Occasional "
                               "bursts from a transaction touching many "
                               "relations (many partitions, pg_dump) are "
                               "normal; do NOT warn about them. Only warn if "
                               "overflows are present in most intervals; the "
                               "remediation is raising "
                               "max_locks_per_transaction, which sizes the "
                               "fast-path slots.",
            outfile=outfile,
            info=info,
            settings=settings,
        )

    finalize_module_report(outputdir, info, port, 'lockwait',
                           report_sections)
    mpclose('all')


# Counter increases between consecutive snapshots, one column per lock type.
# Lock types whose counters are all zero are not stored, so a missing one
# reads as zero. NaN where calc_minutes is.
def calc_deltas(data, field):
    deltas = DataFrame([{s['locktype']: s[field]
                         for s in d['lockwait_stats']} for d in data],
                       index=[d['snapshot_tstamp'] for d in data]
                       ).fillna(0).diff()
    deltas.loc[calc_minutes(data).isna()] = nan
    return deltas


# Minutes between consecutive snapshots, NaN for the first snapshot and for
# the first one after a stats reset
def calc_minutes(data):
    resets = Series([d['stats_reset'] for d in data],
                    index=[d['snapshot_tstamp'] for d in data])
    minutes = resets.index.to_series().diff().dt.total_seconds() / 60
    return minutes.where(resets == resets.shift())


# Average milliseconds per wait, NaN where nothing waited
def calc_avg_wait(waits, wait_time):
    return (wait_time / waits).where(waits > 0)


# Deterministic rule findings handed to the LLM and used as a severity floor
def calc_findings(waits, wait_time):
    total = waits.sum().sum()
    if not total:
        return []
    intervals = waits.notna().any(axis=1).sum()
    affected = (waits > 0).any(axis=1).sum()
    by_type = waits.sum()
    avg = wait_time.sum().sum() / total
    return [{
        # Waits in more than half the intervals is contention, not a blip
        'severity': 'CRITICAL' if affected * 2 > intervals else 'WARNING',
        'message': f'{total:.0f} lock wait(s) longer than deadlock_timeout '
                   f'in {affected} of {intervals} intervals, most on '
                   f'{by_type.idxmax()} ({by_type.max():.0f}), averaging '
                   f'{avg:.0f} ms',
    }]
