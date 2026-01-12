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
from pg_statviz.libs import plot
from pg_statviz.libs.dbconn import dbconn
from pg_statviz.libs.info import getinfo


def get_config_diff(prev_conf, curr_conf):
    """Return dict of changed params with old/new values
    Skip if prev_conf is empty (baseline)"""
    if not prev_conf:
        return {}
    changes = {}
    all_keys = set(prev_conf.keys()) | set(curr_conf.keys())
    for key in all_keys:
        old_val = prev_conf.get(key)
        new_val = curr_conf.get(key)
        if old_val != new_val:
            changes[key] = {'old': old_val, 'new': new_val}
    return changes


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
def conf(*, dbname=getpass.getuser(), host="/var/run/postgresql", port="5432",
         username=getpass.getuser(), password=False, daterange=[],
         outputdir=None, info=None, conn=None):
    "run configuration changes analysis module"

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

    _logger.info("Running configuration changes analysis")

    if daterange:
        daterange = [isoparse(d) for d in daterange]
        if daterange[0] > daterange[1]:
            daterange = [daterange[1], daterange[0]]
    else:
        daterange = ['-infinity', 'now()']

    cur = conn.cursor()

    # Get baseline config (first config <= start of range)
    cur.execute("""SELECT conf, snapshot_tstamp
                   FROM pgstatviz.conf
                   WHERE snapshot_tstamp <= %s
                   ORDER BY snapshot_tstamp DESC
                   LIMIT 1""",
                (daterange[0],))
    baseline = cur.fetchone()

    # Get config changes within the date range
    cur.execute("""SELECT conf, snapshot_tstamp
                   FROM pgstatviz.conf
                   WHERE snapshot_tstamp BETWEEN %s AND %s
                   ORDER BY snapshot_tstamp""",
                (daterange[0], daterange[1]))
    data = cur.fetchall()

    if not data and not baseline:
        _logger.warning("No config snapshots found, skipping")
        return

    # Build list of changes with diffs
    changes = []
    prev_conf = baseline['conf'] if baseline else {}

    for row in data:
        diff = get_config_diff(prev_conf, row['conf'])
        if diff:
            changes.append({
                'timestamp': row['snapshot_tstamp'],
                'diff': diff
            })
        prev_conf = row['conf']

    if not changes:
        _logger.warning("No configuration changes in date range, skipping")
        return

    # Plot configuration changes timeline
    plt, fig = plot.setup()
    plt.suptitle(f"pg_statviz · {info['hostname']}:{port}",
                 fontweight='semibold')
    plt.title("Configuration changes")

    # Get colors (axvline doesn't auto-cycle like plot_date)
    colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

    # Plot vertical lines at each change with legend labels
    for i, change in enumerate(changes):
        ts_str = change['timestamp'].strftime('%Y-%m-%d %H:%M')
        diff_parts = []
        for param, vals in change['diff'].items():
            old = vals['old'] if vals['old'] is not None else 'NULL'
            new = vals['new'] if vals['new'] is not None else 'NULL'
            diff_parts.append(f"{param}: {old} -> {new}")
        label = f"{ts_str}: {', '.join(diff_parts)}"
        plt.axvline(x=change['timestamp'], color=colors[i % len(colors)],
                    linestyle='--', linewidth=1.5, alpha=0.7, label=label)

    plt.xlabel("Timestamp", fontweight='semibold')
    # Pad the x-axis
    plt.margins(x=0.05)

    # Hide y-axis
    plt.gca().get_yaxis().set_visible(False)

    fig.legend()
    fig.tight_layout()
    outfile = f"""{
        outputdir.rstrip("/") + "/" if outputdir
        else ''}pg_statviz_{info['hostname']
                            .replace("/", "-")}_{port}_conf.png"""
    _logger.info(f"Saving {outfile}")
    plt.savefig(outfile)
    mpclose('all')
