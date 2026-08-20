/*
// pg_statviz--1.1--1.2.sql - Upgrade extension to 1.2
*/

-- Add WAL full page image bytes tracking to wal table (PG19+)
ALTER TABLE @extschema@.wal ADD COLUMN wal_fpi_bytes bigint;

-- Update snapshot_conf to capture PG18/PG19 IO worker, WAL level and
-- autovacuum scoring settings (absent settings are simply skipped)
CREATE OR REPLACE FUNCTION @extschema@.snapshot_conf(snapshot_tstamp timestamptz)
RETURNS void
AS $$
DECLARE
    current_conf jsonb;
    previous_conf jsonb;
BEGIN
    SELECT jsonb_object_agg("variable", "value")
    INTO current_conf
    FROM (
        SELECT "name" AS "variable",
               "setting" AS "value"
        FROM pg_settings
        WHERE "name" IN (
            'autovacuum',
            'autovacuum_max_workers',
            'autovacuum_naptime',
            'autovacuum_work_mem',
            'bgwriter_delay',
            'bgwriter_lru_maxpages',
            'bgwriter_lru_multiplier',
            'checkpoint_completion_target',
            'checkpoint_timeout',
            'max_connections',
            'max_wal_size',
            'max_wal_senders',
            'work_mem',
            'maintenance_work_mem',
            'max_replication_slots',
            'max_parallel_workers',
            'max_parallel_maintenance_workers',
            'server_version_num',
            'shared_buffers',
            'vacuum_cost_delay',
            'vacuum_cost_limit',
            'effective_wal_level',
            'io_min_workers',
            'io_max_workers',
            'io_worker_idle_timeout',
            'io_worker_launch_interval',
            'autovacuum_max_parallel_workers',
            'autovacuum_vacuum_score_weight',
            'autovacuum_vacuum_insert_score_weight',
            'autovacuum_analyze_score_weight',
            'autovacuum_freeze_score_weight',
            'autovacuum_multixact_freeze_score_weight')) s;

    SELECT c1.conf INTO previous_conf
    FROM @extschema@.conf c1
    WHERE c1.snapshot_tstamp = (SELECT MAX(c2.snapshot_tstamp) FROM @extschema@.conf c2);

    IF previous_conf IS NULL OR current_conf IS DISTINCT FROM previous_conf THEN
        INSERT INTO @extschema@.conf (snapshot_tstamp, conf)
        VALUES (snapshot_conf.snapshot_tstamp, current_conf);
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Update snapshot_wal to capture wal_fpi_bytes on PG19+
DO $block$
BEGIN
    IF (SELECT current_setting('server_version_num')::int >= 190000) THEN
        -- PG19 adds wal_fpi_bytes to pg_stat_wal
        CREATE OR REPLACE FUNCTION @extschema@.snapshot_wal(snapshot_tstamp timestamptz)
        RETURNS void
        AS $$
            INSERT INTO @extschema@.wal (
                    snapshot_tstamp,
                    wal_records,
                    wal_fpi,
                    wal_fpi_bytes,
                    wal_bytes,
                    wal_buffers_full,
                    wal_write,
                    wal_sync,
                    wal_write_time,
                    wal_sync_time,
                    stats_reset)
                SELECT
                    snapshot_tstamp,
                    w.wal_records,
                    w.wal_fpi,
                    w.wal_fpi_bytes,
                    w.wal_bytes,
                    w.wal_buffers_full,
                    SUM(io.writes),
                    SUM(io.fsyncs),
                    SUM(io.write_time),
                    SUM(io.fsync_time),
                    w.stats_reset
                FROM pg_stat_wal w, pg_stat_io io
                WHERE io.object = 'wal'
                GROUP BY w.wal_records, w.wal_fpi, w.wal_fpi_bytes, w.wal_bytes, w.wal_buffers_full, w.stats_reset;
        $$ LANGUAGE SQL;
    END IF;
END
$block$ LANGUAGE PLPGSQL;


-- Blocking locks
CREATE TABLE IF NOT EXISTS @extschema@.blocking(
    snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
    blocked_total int,
    blockers_total int,
    blocking jsonb);

CREATE OR REPLACE FUNCTION @extschema@.snapshot_blocking(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    WITH
        blk AS (
            -- pg_blocking_pids() resolves the wait graph itself, including
            -- soft blocks from sessions merely ahead in the lock queue
            SELECT DISTINCT
                blocked.pid AS blocked_pid,
                l.locktype AS lock_type,
                bp.pid AS blocking_pid
            FROM pg_catalog.pg_stat_activity blocked
            JOIN pg_catalog.pg_locks l
                ON l.pid = blocked.pid AND NOT l.granted
            CROSS JOIN LATERAL unnest(pg_blocking_pids(blocked.pid)) AS bp(pid)
            WHERE blocked.datname = current_database()
            AND blocked.pid != pg_backend_pid()), -- ignore snapshot session
        blocks AS (
            SELECT coalesce(jsonb_agg(b), '[]'::jsonb)
            FROM (
                SELECT lock_type, count(DISTINCT blocked_pid) AS blocked_count
                FROM blk
                GROUP BY lock_type) b)
    INSERT INTO @extschema@.blocking (
        snapshot_tstamp,
        blocked_total,
        blockers_total,
        blocking)
    SELECT
        snapshot_tstamp,
        count(DISTINCT blocked_pid) AS blocked_total,
        count(DISTINCT blocking_pid) AS blockers_total,
        (SELECT * from blocks) AS blocking
    FROM blk;
$$ LANGUAGE SQL;


-- Add blocking locks to the snapshot function
CREATE OR REPLACE FUNCTION @extschema@.snapshot()
RETURNS timestamptz
AS $$
    DECLARE ts timestamptz;
    BEGIN
        ts := clock_timestamp();
        INSERT INTO @extschema@.snapshots
        VALUES (ts);
        PERFORM @extschema@.snapshot_buf(ts);
        PERFORM @extschema@.snapshot_conf(ts);
        PERFORM @extschema@.snapshot_conn(ts);
        PERFORM @extschema@.snapshot_db(ts);
        -- pg_stat_io only exists in PG16+
        IF (SELECT current_setting('server_version_num')::int >= 160000) THEN
            PERFORM @extschema@.snapshot_io(ts);
        END IF;
        PERFORM @extschema@.snapshot_lock(ts);
        PERFORM @extschema@.snapshot_blocking(ts);
        PERFORM @extschema@.snapshot_repl(ts);
        PERFORM @extschema@.snapshot_slru(ts);
        PERFORM @extschema@.snapshot_wait(ts);
        -- pg_stat_wal only exists in PG14+
        IF (SELECT current_setting('server_version_num')::int >= 140000) THEN
            PERFORM @extschema@.snapshot_wal(ts);
        END IF;
        RAISE NOTICE 'created pg_statviz snapshot';
        RETURN ts;
    END
$$ LANGUAGE PLPGSQL;


GRANT SELECT, INSERT, DELETE, TRUNCATE ON @extschema@.blocking TO pg_monitor;

SELECT pg_catalog.pg_extension_config_dump('pgstatviz.blocking', '');
