CREATE EXTENSION pg_statviz;
SELECT 1 FROM pgstatviz.snapshot();
SELECT count(*)
    FROM pgstatviz.conn t
    JOIN pgstatviz.snapshots s USING (snapshot_tstamp);
-- stats_reset is recorded even when no lock type has waited
SELECT (count(stats_reset) = 1)
           = (current_setting('server_version_num')::int >= 190000)
    AS lockwait_ok
    FROM pgstatviz.lockwait;
-- Every function exists on every version, so none is missing after pg_upgrade
SELECT proname
    FROM pg_proc
    WHERE pronamespace = 'pgstatviz'::regnamespace
    ORDER BY 1;
SELECT (SELECT count(*) = 1 FROM pgstatviz.buf) AS buf_ok,
       (SELECT count(*) = 1 FROM pgstatviz.io)
           = (current_setting('server_version_num')::int >= 160000) AS io_ok,
       (SELECT count(*) = 1 FROM pgstatviz.wal)
           = (current_setting('server_version_num')::int >= 140000) AS wal_ok;
