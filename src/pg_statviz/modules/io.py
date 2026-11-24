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
def io(*, dbname=getpass.getuser(), host="/var/run/postgresql", port="5432",
       username=getpass.getuser(), password=None, daterange=[],
       outputdir=None, ai=None, info=None, conn=None):
    "run I/O analysis module"

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

    data = cur.fetchall()
    if not data:
        cur.execute("""SELECT
                    (current_setting('server_version_num')::int >= 160000)
                    AS version_ok""")
        versioncheck = cur.fetchone()['version_ok']
        if not versioncheck:
            _logger.warning("I/O analysis is only available from "
                            + "PostgreSQL release 16 onwards")
            return
        else:
            raise SystemExit("No pg_statviz snapshots found in this database")

    tstamps = [ts['snapshot_tstamp'] for ts in data]
    blcksz = int(data[0]['block_size'])
    iostats = calc_iostats(data, blcksz)
    iorates = calc_iorates(data, iostats)
    settings = get_settings(conn, ['io_method', 'io_workers', 'io_min_workers',
                                   'io_max_workers', 'io_worker_idle_timeout',
                                   'io_worker_launch_interval'])

    # Build a flattened DataFrame for AI analysis
    io_df = build_io_dataframe(iostats, 1073741824, 'GB', tstamps)

    report_sections = []

    # Plot reads and writes of each I/O kind per snapshot
    plt, fig, splt1, splt2 = plot.setupdouble()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plot_iokinds(splt1, iostats['read'], tstamps, "I/O Reads",
                 "{} read (at time of snapshot)")
    plot_iokinds(splt2, iostats['write'], tstamps, "I/O Writes",
                 "{} written (at time of snapshot)")
    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_io.png"""
    _logger.info(f"Saving {outfile}")
    plt.gcf().autofmt_xdate()
    fig.tight_layout()
    plt.savefig(outfile)
    run_chart_analysis(
        report_sections, ai, io_df, "I/O Statistics",
        metric_description="CUMULATIVE COUNTER — rising values are "
                           "NORMAL. I/O volume by component. "
                           "'client backend' is query work. "
                           "'checkpointer' spikes at checkpoint "
                           "intervals are normal. No warning "
                           "threshold. Default to [HEALTHY].",
        outfile=outfile,
        info=info,
        settings=settings,
    )

    # Build rate DataFrame for AI analysis
    rate_df = build_io_dataframe(iorates, 1048576, 'MBps', tstamps)

    # Plot I/O Rates
    plt, fig, splt1, splt2 = plot.setupdouble()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plot_iokinds(splt1, iorates['read'], tstamps, "I/O Read Rate",
                 "Avg. read rate in {}/s")
    plot_iokinds(splt2, iorates['write'], tstamps, "I/O Write Rate",
                 "Avg. write rate in {}/s")
    plt.gcf().autofmt_xdate()
    fig.tight_layout()
    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_io_rate.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)
    run_chart_analysis(
        report_sections, ai, rate_df, "I/O Rate",
        metric_description="I/O RATES in MB/s (derived from cumulative "
                           "counters). NaN values appear on "
                           "stats_reset — IGNORE them. Read spikes "
                           "indicate cache misses. Write spikes "
                           "during checkpoints are normal. No "
                           "warning threshold (storage limits vary). "
                           "Default to [HEALTHY].",
        outfile=outfile,
        info=info,
        settings=settings,
    )

    finalize_module_report(outputdir, info, port, 'io',
                           report_sections)
    mpclose('all')


# Gather I/O stats in bytes: for reads and writes, a list per I/O kind with
# one value per snapshot. A kind missing from a snapshot has done no I/O yet.
def calc_iostats(data, blcksz=8192):
    iostats = {'read': {}, 'write': {}}
    for i, snapshot in enumerate(data):
        for entry in snapshot['io_stats']:
            name = iokind_name(entry)
            for rw in iostats:
                if name not in iostats[rw]:
                    iostats[rw][name] = [0] * len(data)
                iostats[rw][name][i] = iobytes(entry, rw, blcksz)
    return iostats


# Calculate I/O rates in bytes/s
def calc_iorates(data, iostats):

    # I/O diff generator - yields list of rates in bytes/s
    def iodiff(values):
        yield numpy.nan
        for i in range(len(data) - 1):
            if data[i + 1]['stats_reset'] == data[i]['stats_reset']:
                s = (data[i + 1]['snapshot_tstamp']
                     - data[i]['snapshot_tstamp']).total_seconds()
                yield (values[i + 1] - values[i]) / s
            else:
                yield numpy.nan

    rates = {}
    for rw in iostats:
        rates[rw] = {}
        for name, values in iostats[rw].items():
            rates[rw][name] = list(iodiff(values))
    return rates


# Bytes read or written: PG18+ reports them, older releases count blocks.
# NULL, where the operation does not apply to the I/O kind, counts as 0
def iobytes(entry, rw, blcksz):
    if f"{rw}_bytes" in entry:
        return int(entry[f"{rw}_bytes"] or 0)
    return int(entry[f"{rw}s"] or 0) * blcksz


# Chart label of an I/O kind, naming the object unless it is a relation
def iokind_name(iokind):
    prefix = '' if iokind['object'] == 'relation' else f"{iokind['object']}/"
    return f"{prefix}{iokind['backend_type']}/{iokind['context']}"


# Plot each I/O kind with a non-zero value, in the largest unit from KB to TB
# in which the biggest value is at least 1
def plot_iokinds(splt, series, tstamps, title, ylabel):
    biggest = 0
    for values in series.values():
        biggest = numpy.nanmax([biggest, *values])
    unit, size = 'KB', 1024
    for u, power in (('MB', 2), ('GB', 3), ('TB', 4)):
        if biggest >= 1024 ** power:
            unit, size = u, 1024 ** power
    for name, values in series.items():
        if not all(numpy.isnan(v) or v == 0 for v in values):
            # Downsample if needed
            _frame = DataFrame(data=[v / size for v in values],
                               index=tstamps, copy=False)
            if len(tstamps) > plot.MAX_POINTS:
                q = str(round(
                    (tstamps[-1] - tstamps[0]).total_seconds()
                    / plot.MAX_POINTS, 2))
                r = _frame.resample(q + "s").mean()
            else:
                r = _frame
            splt.plot(r.index, r, label=name)
    splt.set_title(title)
    splt.set_xlabel("Timestamp", fontweight='semibold')
    splt.set_ylabel(ylabel.format(unit), fontweight='semibold')
    splt.set_ylim(bottom=0)
    splt.legend()


# Build a flattened DataFrame of reads and writes per I/O kind for AI
# analysis, leaving out the series that stay zero
def build_io_dataframe(series, size, unit, tstamps):
    data = {}
    for rw in series:
        for name, values in series[rw].items():
            column = [0 if numpy.isnan(v) else round(v / size, 2)
                      for v in values]
            if any(column):
                data[f"{name}_{rw}_{unit}"] = column
    return DataFrame(data=data, index=tstamps, copy=False)
