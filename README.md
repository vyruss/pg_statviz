# pg_statviz

`pg_statviz` is a minimalist extension and utility pair for time series analysis and visualization
of PostgreSQL internal statistics.

Created for snapshotting PostgreSQL's cumulative and dynamic statistics and performing time series
analysis on them. The accompanying utility can produce visualizations for selected time ranges on
the stored stats snapshots, enabling the user to track PostgreSQL performance over time and
potentially perform tuning or troubleshooting.

## Design Philosophy

Designed with the [K.I.S.S.](https://en.wikipedia.org/wiki/KISS_principle) and
[UNIX](https://en.wikipedia.org/wiki/Unix_philosophy) philosophies in mind, `pg_statviz` aims to be
a modular, minimal and unobtrusive tool that does only what it's meant for: create snapshots
of PostgreSQL statistics for visualization and analysis. To this end, a utility is provided for
retrieving the stored snapshots and creating with them simple visualizations using
[Matplotlib](https://github.com/matplotlib/matplotlib).

## Installation

### Extension

The extension can be installed like this, for example inside `psql`:

```sql
CREATE EXTENSION pg_statviz;
```

This will create the needed tables and functions under schema `pgstatviz` (note the lack of 
underscore in the schema name).

### Utility

The visualization utility can be installed from [PyPi](https://pypi.org/project/pg_statviz/):

```shell
pip install pg_statviz
```

### Requirements

Python 3.9+ is required for the visualization utility.

## Usage

The extension can be used by superusers, or any user that has `pg_monitor` role privileges. To take 
a snapshot, e.g. from `psql`:

```sql
SELECT pgstatviz.snapshot();
```
```sql
NOTICE:  created pg_statviz snapshot
           snapshot
-------------------------------

 2023-01-27 11:04:58.055453+00

(1 row)
```

Older snapshots and their associated data can be removed using any time expression. For example, to 
remove data more than 90 days old:

```sql
DELETE FROM pgstatviz.snapshots
WHERE snapshot_tstamp < CURRENT_DATE - 90;
```

Or all snapshots can be removed like this:

```sql
SELECT pgstatviz.delete_snapshots();
```
```sql
NOTICE:  truncating table "snapshots"
NOTICE:  truncate cascades to table "conf"
NOTICE:  truncate cascades to table "buf"
NOTICE:  truncate cascades to table "conn"     
NOTICE:  truncate cascades to table "lock"
NOTICE:  truncate cascades to table "wait"                
NOTICE:  truncate cascades to table "wal"
NOTICE:  truncate cascades to table "db"
 delete_snapshots 
------------------

(1 row)
```

The `pg_monitor` role can be assigned to any user:

```sql
GRANT pg_monitor TO myuser;
```

## Scheduling

Periodic snapshots can be set up with any job scheduler. For example with `cron`:

```shell
crontab -e -u postgres
```

Inside the `postgres` user's crontab, add this line to take a snapshot every 15 minutes:

```
*/15 * * * * psql -c "SELECT pgstatviz.snapshot()" >/dev/null 2>&1
```

## Visualization

The visualization utility can be called like a PostgreSQL command line tool:

```shell
pg_statviz --help
```
```
usage: pg_statviz [--help] [--version] [-d DBNAME] [-h HOSTNAME] [-p PORT] [-U USERNAME] [-W]
                  [-D FROM TO] [-O OUTPUTDIR]
                  {analyze,buf,cache,checkp,conn,lock,tuple,wait,wal} ...

run all analysis modules

positional arguments:
  {analyze,buf,cache,checkp,conn,tuple,wait,wal}
    analyze             run all analysis modules
    buf                 run buffers written analysis module
    cache               run cache hit ratio analysis module
    checkp              run checkpoint analysis module
    conn                run connection count analysis module
    lock                run locks analysis module
    tuple               run tuple count analysis module
    wait                run wait events analysis module
    wal                 run WAL generation analysis module

options:
  --help
  --version             show program's version number and exit
  -d DBNAME, --dbname DBNAME
                        database name to analyze (default: 'myuser')
  -h HOSTNAME, --host HOSTNAME
                        database server host or socket directory (default: '/var/run/postgresql')
  -p PORT, --port PORT  database server port (default: '5432')
  -U USERNAME, --username USERNAME
                        database user name (default: 'myuser')
  -W, --password        force password prompt (should happen automatically) (default: False)
  -D FROM TO, --daterange FROM TO
                        date range to be analyzed in ISO 8601 format e.g. 2023-01-01T00:00
                        2023-01-01T23:59 (default: [])
  -O OUTPUTDIR, --outputdir OUTPUTDIR
                        output directory (default: -)
```

### Specific module usage

```shell
pg_statviz conn --help
```
```
usage: pg_statviz conn [-h] [-d DBNAME] [--host HOSTNAME] [-p PORT] [-U USERNAME] [-W]
                       [-D FROM TO] [-O OUTPUTDIR] [-u [USERS ...]]

run connection count analysis module

options:
  -h, --help            show this help message and exit
  -d DBNAME, --dbname DBNAME
                        database name to analyze (default: 'myuser')
  --host HOSTNAME       database server host or socket directory (default: '/var/run/postgresql')
  -p PORT, --port PORT  database server port (default: '5432')
  -U USERNAME, --username USERNAME
                        database user name (default: 'myuser')
  -W, --password        force password prompt (should happen automatically) (default: False)
  -D FROM TO, --daterange FROM TO
                        date range to be analyzed in ISO 8601 format e.g. 2023-01-01T00:00
                        2023-01-01T23:59 (default: [])
  -O OUTPUTDIR, --outputdir OUTPUTDIR
                        output directory (default: -)
  -u [USERS ...], --users [USERS ...]
                        user name(s) to plot in analysis (default: [])
```

### Example:

```shell
pg_statviz buf --host localhost -d postgres -U postgres -D 2023-01-24T23:00 2023-01-26
```

### Produces:
![buf output sample](src/pg_statviz/libs/pg_statviz_localhost_5432_buf.png)
![buf output sample (rate)](src/pg_statviz/libs/pg_statviz_localhost_5432_buf_rate.png)


## Export data

Data from `pg_statviz` internal tables can be exported to a tab separated values (TSV) file for use 
by other tools:

```shell
psql -c "COPY pgstatviz.conn TO STDOUT CSV HEADER DELIMITER E'\t'" > conn.tsv
```
