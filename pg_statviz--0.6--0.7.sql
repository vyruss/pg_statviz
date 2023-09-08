/*
// pg_statviz--0.6--0.7.sql - Upgrade extension to 0.7
*/

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
                        fsync_time
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


SELECT pg_catalog.pg_extension_config_dump('pgstatviz.io', '');
