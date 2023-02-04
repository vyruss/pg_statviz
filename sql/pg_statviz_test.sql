CREATE EXTENSION pg_statviz;
SELECT 1 FROM pgstatviz.snapshot();
SELECT count(*)
    FROM pgstatviz.conn t
    JOIN pgstatviz.snapshots s USING (snapshot_tstamp);
