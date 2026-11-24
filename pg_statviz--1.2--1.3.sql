/*
// pg_statviz--1.2--1.3.sql - Upgrade extension to 1.3
*/

-- Update snapshot_conf to capture lock, I/O method, SLRU cache, WAL and
-- replication slot settings
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
            'io_method',
            'io_workers',
            'io_min_workers',
            'io_max_workers',
            'io_worker_idle_timeout',
            'io_worker_launch_interval',
            'autovacuum_max_parallel_workers',
            'autovacuum_vacuum_score_weight',
            'autovacuum_vacuum_insert_score_weight',
            'autovacuum_analyze_score_weight',
            'autovacuum_freeze_score_weight',
            'autovacuum_multixact_freeze_score_weight',
            'deadlock_timeout',
            'idle_in_transaction_session_timeout',
            'lock_timeout',
            'log_lock_waits',
            'max_locks_per_transaction',
            'transaction_buffers',
            'subtransaction_buffers',
            'multixact_offset_buffers',
            'multixact_member_buffers',
            'commit_timestamp_buffers',
            'notify_buffers',
            'serializable_buffers',
            'wal_compression',
            'full_page_writes',
            'max_slot_wal_keep_size',
            'wal_keep_size',
            'idle_replication_slot_timeout')) s;

    SELECT c1.conf INTO previous_conf
    FROM @extschema@.conf c1
    WHERE c1.snapshot_tstamp = (SELECT MAX(c2.snapshot_tstamp) FROM @extschema@.conf c2);

    IF previous_conf IS NULL OR current_conf IS DISTINCT FROM previous_conf THEN
        INSERT INTO @extschema@.conf (snapshot_tstamp, conf)
        VALUES (snapshot_conf.snapshot_tstamp, current_conf);
    END IF;
END;
$$ LANGUAGE plpgsql;


-- Leave out SLRUs whose counters are all zero
CREATE OR REPLACE FUNCTION @extschema@.snapshot_slru(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    INSERT INTO @extschema@.slru (
        snapshot_tstamp,
        slru_stats)
    SELECT
        snapshot_tstamp,
        jsonb_agg(jsonb_build_object(
            'name', name,
            'blks_zeroed', blks_zeroed,
            'blks_hit', blks_hit,
            'blks_read', blks_read,
            'blks_written', blks_written,
            'blks_exists', blks_exists,
            'flushes', flushes,
            'truncates', truncates
        ))
    FROM pg_stat_slru
    -- SLRUs whose counters are all zero are left out
    WHERE blks_zeroed + blks_hit + blks_read + blks_written + blks_exists
        + flushes + truncates > 0;
$$ LANGUAGE SQL;


-- Lock wait statistics
CREATE TABLE IF NOT EXISTS @extschema@.lockwait(
    snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
    lockwait_stats jsonb,
    stats_reset timestamptz);

-- pg_stat_lock only exists in PG19+
CREATE OR REPLACE FUNCTION @extschema@.snapshot_lockwait(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    WITH
        pgsl AS (
            SELECT
                locktype,
                waits,
                wait_time,
                fastpath_exceeded,
                stats_reset
            FROM pg_stat_lock)
    INSERT INTO @extschema@.lockwait (
        snapshot_tstamp,
        lockwait_stats,
        stats_reset)
    SELECT
        snapshot_tstamp,
        -- lock types whose counters are all zero are left out
        (SELECT coalesce(jsonb_agg(jsonb_build_object(
                    'locktype', locktype,
                    'waits', waits,
                    'wait_time', wait_time,
                    'fastpath_exceeded', fastpath_exceeded)), '[]')
            FROM pgsl
            WHERE waits > 0 OR wait_time > 0 OR fastpath_exceeded > 0),
        -- from every lock type, so it is recorded when none has waited
        (SELECT max(stats_reset) FROM pgsl);
$$ LANGUAGE SQL;


-- Stats that differ between server versions: a function for each version,
-- created on every version, replacing the ones created only for the server
-- version the extension was installed on

-- PG17+ moved things out of pg_stat_bgwriter
CREATE OR REPLACE FUNCTION @extschema@.snapshot_buf_pg17(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    INSERT INTO @extschema@.buf (
        snapshot_tstamp,
        checkpoints_timed,
        checkpoints_req,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        buffers_clean,
        maxwritten_clean,
        buffers_backend,
        buffers_backend_fsync,
        buffers_alloc,
        stats_reset)
    SELECT
        snapshot_tstamp,
        c.num_timed,
        c.num_requested,
        c.write_time,
        c.sync_time,
        c.buffers_written,
        b.buffers_clean,
        b.maxwritten_clean,
        i.writes,
        i.fsyncs,
        b.buffers_alloc,
        b.stats_reset
    FROM pg_stat_bgwriter b, pg_stat_checkpointer c, pg_stat_io i
    WHERE i.backend_type = 'client backend'
    AND i.context = 'normal'
    AND i.object = 'relation';
$$ LANGUAGE SQL;


-- PG13-16 have them all in pg_stat_bgwriter
CREATE OR REPLACE FUNCTION @extschema@.snapshot_buf_pg13(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    INSERT INTO @extschema@.buf (
        snapshot_tstamp,
        checkpoints_timed,
        checkpoints_req,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        buffers_clean,
        maxwritten_clean,
        buffers_backend,
        buffers_backend_fsync,
        buffers_alloc,
        stats_reset)
    SELECT
        snapshot_tstamp,
        checkpoints_timed,
        checkpoints_req,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        buffers_clean,
        maxwritten_clean,
        buffers_backend,
        buffers_backend_fsync,
        buffers_alloc,
        stats_reset
    FROM pg_stat_bgwriter;
$$ LANGUAGE SQL;


-- PG18+ uses byte-based metrics (read_bytes, write_bytes, extend_bytes)
CREATE OR REPLACE FUNCTION @extschema@.snapshot_io_pg18(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    WITH
        pgsi AS (
            SELECT
                backend_type,
                object,
                context,
                reads,
                read_time,
                read_bytes,
                writes,
                write_time,
                write_bytes,
                writebacks,
                writeback_time,
                extends,
                extend_time,
                extend_bytes,
                hits,
                evictions,
                reuses,
                fsyncs,
                fsync_time,
                stats_reset
            FROM pg_stat_io
            WHERE NOT (reads = 0 AND writes = 0)),
        ioagg AS (
            SELECT jsonb_agg(io)
            FROM (SELECT *
                  FROM pgsi) io)
    INSERT INTO @extschema@.io (
            snapshot_tstamp,
            io_stats,
            stats_reset)
    SELECT snapshot_tstamp,
           (SELECT * FROM ioagg) AS io_stats,
           (SELECT stats_reset FROM pgsi LIMIT 1) AS stats_reset;
$$ LANGUAGE SQL;


-- PG16-17 uses operation counts without byte metrics
CREATE OR REPLACE FUNCTION @extschema@.snapshot_io_pg16(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    WITH
        pgsi AS (
            SELECT
                backend_type,
                object,
                context,
                reads,
                read_time,
                writes,
                write_time,
                writebacks,
                writeback_time,
                extends,
                extend_time,
                hits,
                evictions,
                reuses,
                fsyncs,
                fsync_time,
                stats_reset
            FROM pg_stat_io
            WHERE NOT (reads = 0 AND writes = 0)),
        ioagg AS (
            SELECT jsonb_agg(io)
            FROM (SELECT *
                  FROM pgsi) io)
    INSERT INTO @extschema@.io (
            snapshot_tstamp,
            io_stats,
            stats_reset)
    SELECT snapshot_tstamp,
           (SELECT * FROM ioagg) AS io_stats,
           (SELECT stats_reset FROM pgsi LIMIT 1) AS stats_reset;
$$ LANGUAGE SQL;


-- PG19 adds wal_fpi_bytes to pg_stat_wal
CREATE OR REPLACE FUNCTION @extschema@.snapshot_wal_pg19(snapshot_tstamp timestamptz)
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


-- PG18 moved wal_write/wal_sync statistics to pg_stat_io (object = 'wal')
CREATE OR REPLACE FUNCTION @extschema@.snapshot_wal_pg18(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    INSERT INTO @extschema@.wal (
            snapshot_tstamp,
            wal_records,
            wal_fpi,
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
            w.wal_bytes,
            w.wal_buffers_full,
            SUM(io.writes),
            SUM(io.fsyncs),
            SUM(io.write_time),
            SUM(io.fsync_time),
            w.stats_reset
        FROM pg_stat_wal w, pg_stat_io io
        WHERE io.object = 'wal'
        GROUP BY w.wal_records, w.wal_fpi, w.wal_bytes, w.wal_buffers_full, w.stats_reset;
$$ LANGUAGE SQL;


-- PG14-17 has all WAL stats in pg_stat_wal
CREATE OR REPLACE FUNCTION @extschema@.snapshot_wal_pg14(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    INSERT INTO @extschema@.wal (
            snapshot_tstamp,
            wal_records,
            wal_fpi,
            wal_bytes,
            wal_buffers_full,
            wal_write,
            wal_sync,
            wal_write_time,
            wal_sync_time,
            stats_reset)
        SELECT
            snapshot_tstamp,
            wal_records,
            wal_fpi,
            wal_bytes,
            wal_buffers_full,
            wal_write,
            wal_sync,
            wal_write_time,
            wal_sync_time,
            stats_reset
        FROM pg_stat_wal;
$$ LANGUAGE SQL;


-- Add lock wait statistics, and call the function for the running server
-- version
CREATE OR REPLACE FUNCTION @extschema@.snapshot()
RETURNS timestamptz
AS $$
    DECLARE ts timestamptz;
    BEGIN
        ts := clock_timestamp();
        INSERT INTO @extschema@.snapshots
        VALUES (ts);
        -- Stats that differ between server versions have a function for each
        -- version, all created on every version so they exist after pg_upgrade
        IF (SELECT current_setting('server_version_num')::int >= 170000) THEN
            PERFORM @extschema@.snapshot_buf_pg17(ts);
        ELSE
            PERFORM @extschema@.snapshot_buf_pg13(ts);
        END IF;
        PERFORM @extschema@.snapshot_conf(ts);
        PERFORM @extschema@.snapshot_conn(ts);
        PERFORM @extschema@.snapshot_db(ts);
        -- pg_stat_io only exists in PG16+
        IF (SELECT current_setting('server_version_num')::int >= 180000) THEN
            PERFORM @extschema@.snapshot_io_pg18(ts);
        ELSIF (SELECT current_setting('server_version_num')::int >= 160000) THEN
            PERFORM @extschema@.snapshot_io_pg16(ts);
        END IF;
        PERFORM @extschema@.snapshot_lock(ts);
        PERFORM @extschema@.snapshot_blocking(ts);
        -- pg_stat_lock only exists in PG19+
        IF (SELECT current_setting('server_version_num')::int >= 190000) THEN
            PERFORM @extschema@.snapshot_lockwait(ts);
        END IF;
        PERFORM @extschema@.snapshot_repl(ts);
        PERFORM @extschema@.snapshot_slru(ts);
        PERFORM @extschema@.snapshot_wait(ts);
        -- pg_stat_wal only exists in PG14+
        IF (SELECT current_setting('server_version_num')::int >= 190000) THEN
            PERFORM @extschema@.snapshot_wal_pg19(ts);
        ELSIF (SELECT current_setting('server_version_num')::int >= 180000) THEN
            PERFORM @extschema@.snapshot_wal_pg18(ts);
        ELSIF (SELECT current_setting('server_version_num')::int >= 140000) THEN
            PERFORM @extschema@.snapshot_wal_pg14(ts);
        END IF;
        RAISE NOTICE 'created pg_statviz snapshot';
        RETURN ts;
    END
$$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS @extschema@.snapshot_buf(timestamptz);
DROP FUNCTION IF EXISTS @extschema@.snapshot_io(timestamptz);
DROP FUNCTION IF EXISTS @extschema@.snapshot_wal(timestamptz);


GRANT SELECT, INSERT, DELETE, TRUNCATE ON @extschema@.lockwait TO pg_monitor;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA @extschema@ TO pg_monitor;

SELECT pg_catalog.pg_extension_config_dump('pgstatviz.lockwait', '');
