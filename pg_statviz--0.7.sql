/*
// pg_statviz - stats visualization and time series analysis
//
// Copyright (c) 2024 Jimmy Angelakos
// This software is released under the PostgreSQL Licence
//
// pg_statviz--0.7 - Release v0.7
*/

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_statviz" to load this file. \quit


CREATE TABLE IF NOT EXISTS @extschema@.snapshots(
    snapshot_tstamp timestamptz PRIMARY KEY
);


-- Configuration
CREATE TABLE IF NOT EXISTS @extschema@.conf(
    snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
    conf jsonb);

CREATE OR REPLACE FUNCTION @extschema@.snapshot_conf(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    INSERT INTO @extschema@.conf (
        snapshot_tstamp,
        conf)
    SELECT
        snapshot_tstamp,
        jsonb_object_agg("variable", "value")
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
            'vacuum_cost_limit')) s;
$$ LANGUAGE SQL;


-- Buffers and checkpoints
CREATE TABLE IF NOT EXISTS @extschema@.buf(
    snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
    checkpoints_timed bigint,
    checkpoints_req bigint,
    checkpoint_write_time double precision,
    checkpoint_sync_time double precision,
    buffers_checkpoint bigint,
    buffers_clean bigint,
    maxwritten_clean bigint,
    buffers_backend bigint,
    buffers_backend_fsync bigint,
    buffers_alloc bigint,
    stats_reset timestamptz);

-- PG17+ moved things out of pg_stat_bgwriter
DO $block$
BEGIN
    IF (SELECT current_setting('server_version_num')::int >= 170000) THEN
        CREATE OR REPLACE FUNCTION @extschema@.snapshot_buf(snapshot_tstamp timestamptz)
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
    ELSE
        CREATE OR REPLACE FUNCTION @extschema@.snapshot_buf(snapshot_tstamp timestamptz)
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
    END IF;
END
$block$ LANGUAGE PLPGSQL;


-- Connections
CREATE TABLE IF NOT EXISTS @extschema@.conn(
    snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
    conn_total int,
    conn_active int,
    conn_idle int,
    conn_idle_trans int,
    conn_idle_trans_abort int,
    conn_fastpath int,
    conn_users jsonb);

CREATE OR REPLACE FUNCTION @extschema@.snapshot_conn(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    WITH
        pgsa AS (
            SELECT *
            FROM pg_stat_activity
            WHERE datname = current_database()
            AND state IS NOT NULL),
        userconns AS (
            SELECT jsonb_agg(uc)
            FROM (
                SELECT usename AS user, count(*) AS connections
                FROM pgsa
                GROUP BY usename) uc)
    INSERT INTO @extschema@.conn (
        snapshot_tstamp,
        conn_total,
        conn_active,
        conn_idle,
        conn_idle_trans,
        conn_idle_trans_abort,
        conn_fastpath,
        conn_users)
    SELECT
        snapshot_tstamp,
        count(*) AS conn_total,
        count(*) FILTER (WHERE state = 'active') AS conn_active,
        count(*) FILTER (WHERE state = 'idle') AS conn_idle,
        count(*) FILTER (WHERE state = 'idle in transaction') AS conn_idle_trans,
        count(*) FILTER (WHERE state = 'idle in transaction (aborted)') AS conn_idle_trans_abort,
        count(*) FILTER (WHERE state = 'fastpath function call') AS conn_fastpath,
        (SELECT * from userconns) AS conn_users
    FROM pgsa;
$$ LANGUAGE SQL;


-- Locks
CREATE TABLE IF NOT EXISTS @extschema@.lock(
    snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
    locks_total int,
    locks jsonb);

CREATE OR REPLACE FUNCTION @extschema@.snapshot_lock(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    WITH
        pgl AS (
            SELECT *
            FROM pg_locks l, pg_database d
            WHERE d.datname = current_database()
            AND l.database = oid
            AND locktype = 'relation'
            AND pid != pg_backend_pid()), -- ignore snapshot session
        lcks AS (
            SELECT coalesce(jsonb_agg(l), '[]'::jsonb)
            FROM (
                SELECT mode AS lock_mode, count(*) AS lock_count
                FROM pgl
                GROUP BY lock_mode) l)
    INSERT INTO @extschema@.lock (
        snapshot_tstamp,
        locks_total,
        locks)
    SELECT
        snapshot_tstamp,
        count(*) AS locks_total,
        (SELECT * from lcks) AS locks
    FROM pgl;
$$ LANGUAGE SQL;


-- Wait events
CREATE TABLE IF NOT EXISTS @extschema@.wait(
    snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
    wait_events_total int,
    wait_events jsonb);

CREATE OR REPLACE FUNCTION @extschema@.snapshot_wait(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    WITH
        pgsa AS (
            SELECT *
            FROM pg_stat_activity
            WHERE datname = current_database()
            AND state = 'active'
            AND wait_event IS NOT NULL),
        waitevents AS (
            SELECT coalesce(jsonb_agg(we), '[]'::jsonb)
            FROM (
                SELECT wait_event_type, wait_event, count(*) AS wait_event_count
                FROM pgsa
                GROUP BY wait_event_type, wait_event) we)
    INSERT INTO @extschema@.wait (
        snapshot_tstamp,
        wait_events_total,
        wait_events)
    SELECT
        snapshot_tstamp,
        count(*) AS wait_events_total,
        (SELECT * from waitevents) AS wait_events
    FROM pgsa;
$$ LANGUAGE SQL;


-- WAL
CREATE TABLE IF NOT EXISTS @extschema@.wal(
    snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
    wal_records bigint,
    wal_fpi bigint,
    wal_bytes numeric,
    wal_buffers_full bigint,
    wal_write bigint,
    wal_sync bigint,
    wal_write_time double precision,
    wal_sync_time double precision,
    stats_reset timestamptz);

-- pg_stat_wal only exists in PG14+
DO $block$
BEGIN
    IF (SELECT current_setting('server_version_num')::int >= 140000) THEN
        CREATE OR REPLACE FUNCTION @extschema@.snapshot_wal(snapshot_tstamp timestamptz)
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
    END IF;
END
$block$ LANGUAGE PLPGSQL;


-- DB
CREATE TABLE IF NOT EXISTS @extschema@.db(
    snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
    xact_commit bigint,
    xact_rollback bigint,
    blks_read bigint,
    blks_hit bigint,
    tup_returned bigint,
    tup_fetched bigint,
    tup_inserted bigint,
    tup_updated bigint,
    tup_deleted bigint,
    temp_files bigint,
    temp_bytes bigint,
    block_size int,
    stats_reset timestamptz,
    postmaster_start_time timestamptz);

CREATE OR REPLACE FUNCTION @extschema@.snapshot_db(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    INSERT INTO @extschema@.db (
            snapshot_tstamp,
            xact_commit,
            xact_rollback,
            blks_read,
            blks_hit,
            tup_returned,
            tup_fetched,
            tup_inserted,
            tup_updated,
            tup_deleted,
            temp_files,
            temp_bytes,
            stats_reset,
            block_size,
            postmaster_start_time)
        SELECT
            snapshot_tstamp,
            xact_commit,
            xact_rollback,
            blks_read,
            blks_hit,
            tup_returned,
            tup_fetched,
            tup_inserted,
            tup_updated,
            tup_deleted,
            temp_files,
            temp_bytes,
            stats_reset,
            current_setting('block_size')::int,
            pg_postmaster_start_time()
        FROM pg_stat_database
        WHERE datname = current_database();
$$ LANGUAGE SQL;


-- I/O
CREATE TABLE IF NOT EXISTS @extschema@.io(
    snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
    io_stats jsonb,
    stats_reset timestamptz);

-- pg_stat_io only exists in PG16+
DO $block$
BEGIN
    IF (SELECT current_setting('server_version_num')::int >= 160000) THEN
        CREATE OR REPLACE FUNCTION @extschema@.snapshot_io(snapshot_tstamp timestamptz)
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
    END IF;
END
$block$ LANGUAGE PLPGSQL;


-- Snapshots
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
        PERFORM @extschema@.snapshot_wait(ts);
        -- pg_stat_wal only exists in PG14+
        IF (SELECT current_setting('server_version_num')::int >= 140000) THEN
            PERFORM @extschema@.snapshot_wal(ts);
        END IF;
        RAISE NOTICE 'created pg_statviz snapshot';
        RETURN ts;
    END
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION @extschema@.delete_snapshots()
RETURNS void
AS $$
    BEGIN
        RAISE NOTICE 'truncating table "snapshots"';
        TRUNCATE @extschema@.snapshots CASCADE;
    END
$$ LANGUAGE PLPGSQL;


-- Make tables dumpable
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.buf', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.conf', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.conn', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.db', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.io', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.lock', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.snapshots', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.wait', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.wal', '');


-- Permissions
GRANT USAGE ON SCHEMA @extschema@ TO pg_monitor;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA @extschema@ TO pg_monitor;
GRANT SELECT ON ALL TABLES IN SCHEMA @extschema@ TO pg_monitor;
GRANT INSERT ON ALL TABLES IN SCHEMA @extschema@ TO pg_monitor;
GRANT DELETE ON ALL TABLES IN SCHEMA @extschema@ TO pg_monitor;
GRANT TRUNCATE ON ALL TABLES IN SCHEMA @extschema@ TO pg_monitor;
