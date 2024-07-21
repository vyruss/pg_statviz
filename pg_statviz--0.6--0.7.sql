/*
// pg_statviz--0.6--0.7.sql - Upgrade extension to 0.7
*/

-- Buffers and checkpoints

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


-- Configuration
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

-- Convert existing data
CREATE TABLE IF NOT EXISTS @extschema@._upgrade_conf(
    snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
    conf jsonb);

INSERT INTO @extschema@._upgrade_conf
SELECT snapshot_tstamp, jsonb_object_agg(j.x->>'setting', j.x->>'value')
FROM (SELECT * FROM @extschema@.conf) s
CROSS JOIN jsonb_array_elements(conf) AS j(x)
GROUP BY snapshot_tstamp;

DROP TABLE @extschema@.conf;
ALTER TABLE @extschema@._upgrade_conf RENAME TO conf;


-- Connections
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
                WHERE usename IS NOT NULL
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

-- Added block_size
ALTER TABLE @extschema@.db ADD IF NOT EXISTS block_size int;

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
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.io', '');


-- Permissions
GRANT USAGE ON SCHEMA @extschema@ TO pg_monitor;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA @extschema@ TO pg_monitor;
GRANT SELECT ON ALL TABLES IN SCHEMA @extschema@ TO pg_monitor;
GRANT INSERT ON ALL TABLES IN SCHEMA @extschema@ TO pg_monitor;
GRANT DELETE ON ALL TABLES IN SCHEMA @extschema@ TO pg_monitor;
GRANT TRUNCATE ON ALL TABLES IN SCHEMA @extschema@ TO pg_monitor;
