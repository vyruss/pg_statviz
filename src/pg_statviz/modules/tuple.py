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
from pandas import DataFrame
from pg_statviz.libs import plot
from pg_statviz.libs.ai import (AI_PROVIDERS, DEFAULT_AI_PROVIDER,
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
@arg('-A', '--ai', nargs='?', const=DEFAULT_AI_PROVIDER, default=None,
     choices=AI_PROVIDERS, metavar='PROVIDER',
     help="enable AI analysis (default provider: " + DEFAULT_AI_PROVIDER
          + "). Choices: claude (Anthropic), gemini (Google AI Studio), "
            "local (Ollama vision model).")
@arg('--info', help=argparse.SUPPRESS)
@arg('--conn', help=argparse.SUPPRESS)
def tuple(*, dbname=getpass.getuser(), host="/var/run/postgresql", port="5432",
          username=getpass.getuser(), password=None, daterange=[],
          outputdir=None, ai=None, info=None, conn=None):
    "run tuple count analysis module"

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

    _logger.info("Running tuple count analysis")

    if daterange:
        daterange = [isoparse(d) for d in daterange]
        if daterange[0] > daterange[1]:
            daterange = [daterange[1], daterange[0]]
    else:
        daterange = ['-infinity', 'now()']

    cur = conn.cursor()
    cur.execute("""SELECT tup_returned, tup_fetched, tup_inserted, tup_updated,
                          tup_deleted, snapshot_tstamp, stats_reset
                   FROM pgstatviz.db
                   WHERE snapshot_tstamp BETWEEN %s AND %s
                   ORDER BY snapshot_tstamp""",
                (daterange[0], daterange[1]))
    data = cur.fetchall()
    if not data:
        raise SystemExit("No pg_statviz snapshots found in this database")

    tstamps = [t['snapshot_tstamp'] for t in data]
    settings = get_settings(conn, ['autovacuum', 'autovacuum_naptime',
                                   'autovacuum_max_workers',
                                   'autovacuum_work_mem',
                                   'vacuum_cost_delay',
                                   'vacuum_cost_limit'])
    returned = [t['tup_returned'] for t in data]
    fetched = [t['tup_fetched'] for t in data]
    inserted = [t['tup_inserted'] for t in data]
    updated = [t['tup_updated'] for t in data]
    deleted = [t['tup_deleted'] for t in data]
    tuplerates = list(tuplediff(data))

    # Downsample if needed
    tuple_frame = DataFrame(
        data={'returned': returned,
              'fetched': fetched,
              'inserted': inserted,
              'updated': updated,
              'deleted': deleted},
        index=tstamps, copy=False)
    tuplerate_frame = DataFrame(
        data=tuplerates,
        columns=['returned', 'fetched', 'inserted', 'updated', 'deleted'],
        index=tstamps, copy=False)
    if len(tstamps) > plot.MAX_POINTS:
        q = str(round(
            (tstamps[-1] - tstamps[0]).total_seconds() / plot.MAX_POINTS, 2))
        r = tuple_frame.resample(q + "s").mean()
        rr = tuplerate_frame.resample(q + "s").mean()
    else:
        r = tuple_frame
        rr = tuplerate_frame

    report_sections = []

    # Plot tuples read
    plt, fig, splt1, splt2 = plot.setupdouble()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    splt1.set_title("Tuples read")
    splt1.plot(r.index, r['returned'], label="returned")
    splt1.plot(r.index, r['fetched'], label="fetched")
    splt1.set_xlabel("Timestamp", fontweight='semibold')
    splt1.set_ylabel("Tuple count", fontweight='semibold')
    splt1.set_ylim(bottom=0)
    splt1.legend()

    # Plot tuples written
    splt2.set_title("Tuples written")
    splt2.plot(r.index, r['inserted'], label="inserted")
    splt2.plot(r.index, r['updated'], label="updated")
    splt2.plot(r.index, r['deleted'], label="deleted")
    splt2.set_xlabel("Timestamp", fontweight='semibold')
    splt2.set_ylabel("Tuple count", fontweight='semibold')
    splt2.set_ylim(bottom=0)
    splt2.legend()
    plt.gcf().autofmt_xdate()
    fig.tight_layout()

    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_tuple.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)
    run_chart_analysis(
        report_sections, ai, r, "Tuple Statistics",
        metric_description="CUMULATIVE COUNTER — rising values are "
                           "NORMAL. Large deleted+updated generates "
                           "dead tuples requiring vacuum. Large "
                           "returned vs fetched gap suggests seq "
                           "scans with many filtered rows. No "
                           "warning threshold. Default to [HEALTHY].",
        outfile=outfile,
        info=info,
        settings=settings,
    )

    # Plot tuple read rates
    plt, fig, splt1, splt2 = plot.setupdouble()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    splt1.set_title("Tuple read rate")
    splt1.plot(rr.index, rr['returned'],
               label="returned")
    splt1.plot(rr.index, rr['fetched'],
               label="fetched")
    splt1.set_xlabel("Timestamp", fontweight='semibold')
    splt1.set_ylabel("Avg. tuples per minute", fontweight='semibold')
    splt1.set_ylim(bottom=0)
    splt1.legend()

    # Plot tuple write rates
    splt2.set_title("Tuple write rate")
    splt2.plot(rr.index, rr['inserted'],
               label="inserted")
    splt2.plot(rr.index, rr['updated'],
               label="updated")
    splt2.plot(rr.index, rr['deleted'],
               label="deleted")
    splt2.set_xlabel("Timestamp", fontweight='semibold')
    splt2.set_ylabel("Avg. tuples per minute", fontweight='semibold')
    splt2.set_ylim(bottom=0)
    splt2.legend()
    plt.gcf().autofmt_xdate()
    fig.tight_layout()

    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_tuple_rate.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)
    run_chart_analysis(
        report_sections, ai, rr, "Tuple Rate",
        metric_description="Tuple operation RATES (derived from "
                           "cumulative counters). NaN values appear "
                           "on stats_reset — IGNORE them. High "
                           "update/delete rate generates bloat "
                           "faster than autovacuum can clean. No "
                           "warning threshold. Default to [HEALTHY].",
        outfile=outfile,
        info=info,
        settings=settings,
    )

    finalize_module_report(outputdir, info, port, 'tuple',
                           report_sections)
    mpclose('all')


# Tuple diff generator - yields 5-tuple list of the 5 rates in
# tuples/minute
def tuplediff(data):
    yield (numpy.nan, numpy.nan, numpy.nan, numpy.nan, numpy.nan)
    for i, item in enumerate(data):
        if i + 1 < len(data):
            if data[i + 1]['stats_reset'] == data[i]['stats_reset']:
                m = (data[i + 1]['snapshot_tstamp']
                     - data[i]['snapshot_tstamp']).total_seconds() / 60
                yield (round((data[i + 1]['tup_returned']
                              - data[i]['tup_returned']) / m, 1),
                       round((data[i + 1]['tup_fetched']
                              - data[i]['tup_fetched']) / m, 1),
                       round((data[i + 1]['tup_inserted']
                              - data[i]['tup_inserted']) / m, 1),
                       round((data[i + 1]['tup_updated']
                              - data[i]['tup_updated']) / m, 1),
                       round((data[i + 1]['tup_deleted']
                              - data[i]['tup_deleted']) / m, 1))
            else:
                yield (numpy.nan, numpy.nan, numpy.nan, numpy.nan,
                       numpy.nan)
