CREATE EXTENSION pg_statviz;
SELECT 1 FROM pgstatviz.snapshot();
SELECT count(*)
    FROM pgstatviz.conn t
    JOIN pgstatviz.snapshots s USING (snapshot_tstamp);
SELECT (count(*) = 1) = (current_setting('server_version_num')::int >= 190000)
    AS lockwait_ok
    FROM pgstatviz.lockwait;
