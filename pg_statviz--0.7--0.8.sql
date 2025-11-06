/*
// pg_statviz--0.7--0.8.sql - Upgrade extension to 0.8
*/

-- WAL statistics

-- pg_stat_wal only exists in PG14+
-- PG18+ moved wal_write/wal_sync timing statistics to pg_stat_io
DO $block$
BEGIN
    IF (SELECT current_setting('server_version_num')::int >= 180000) THEN
        -- PG18+ moved wal_write/wal_sync statistics to pg_stat_io (object = 'wal')
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
    ELSIF (SELECT current_setting('server_version_num')::int >= 140000) THEN
        -- PG14-17 has all WAL stats in pg_stat_wal
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


-- IO statistics

-- pg_stat_io only exists in PG16+
-- PG18+ added byte-based metrics (read_bytes, write_bytes, extend_bytes)
DO $block$
BEGIN
    IF (SELECT current_setting('server_version_num')::int >= 180000) THEN
        -- PG18+ uses byte-based metrics (read_bytes, write_bytes, extend_bytes)
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
    ELSIF (SELECT current_setting('server_version_num')::int >= 160000) THEN
        -- PG16-17 uses operation counts without byte metrics
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
