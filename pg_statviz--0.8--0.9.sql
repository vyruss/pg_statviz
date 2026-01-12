/*
// pg_statviz--0.8--0.9.sql - Upgrade extension to 0.9
*/

-- Add checksum failure tracking to db table
ALTER TABLE @extschema@.db ADD COLUMN checksum_failures bigint;
ALTER TABLE @extschema@.db ADD COLUMN checksum_last_failure timestamptz;

-- Add session activity age tracking to conn table
ALTER TABLE @extschema@.conn ADD COLUMN max_query_age_seconds double precision;
ALTER TABLE @extschema@.conn ADD COLUMN max_xact_age_seconds double precision;
ALTER TABLE @extschema@.conn ADD COLUMN max_backend_age_seconds double precision;

-- Update snapshot_db function to collect checksum failures
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
            postmaster_start_time,
            checksum_failures,
            checksum_last_failure)
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
            pg_postmaster_start_time(),
            checksum_failures,
            checksum_last_failure
        FROM pg_stat_database
        WHERE datname = current_database();
$$ LANGUAGE SQL;

-- Update snapshot_conn function to collect session ages
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
                GROUP BY usename) uc),
        maxages AS (
            SELECT
                date_part('epoch', max(clock_timestamp() - query_start)) AS max_query_age,
                date_part('epoch', max(clock_timestamp() - xact_start)) AS max_xact_age,
                date_part('epoch', max(clock_timestamp() - backend_start)) AS max_backend_age
            FROM pgsa
            WHERE state != 'idle')
    INSERT INTO @extschema@.conn (
        snapshot_tstamp,
        conn_total,
        conn_active,
        conn_idle,
        conn_idle_trans,
        conn_idle_trans_abort,
        conn_fastpath,
        conn_users,
        max_query_age_seconds,
        max_xact_age_seconds,
        max_backend_age_seconds)
    SELECT
        snapshot_tstamp,
        count(*) AS conn_total,
        count(*) FILTER (WHERE state = 'active') AS conn_active,
        count(*) FILTER (WHERE state = 'idle') AS conn_idle,
        count(*) FILTER (WHERE state = 'idle in transaction') AS conn_idle_trans,
        count(*) FILTER (WHERE state = 'idle in transaction (aborted)') AS conn_idle_trans_abort,
        count(*) FILTER (WHERE state = 'fastpath function call') AS conn_fastpath,
        (SELECT * from userconns) AS conn_users,
        (SELECT max_query_age FROM maxages),
        (SELECT max_xact_age FROM maxages),
        (SELECT max_backend_age FROM maxages)
    FROM pgsa;
$$ LANGUAGE SQL;

-- Add replication stats
CREATE TABLE IF NOT EXISTS @extschema@.repl(
    snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
    standby_lag jsonb,
    slot_stats jsonb);

CREATE OR REPLACE FUNCTION @extschema@.snapshot_repl(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    WITH
        standbys AS (
            SELECT jsonb_agg(jsonb_build_object(
                'application_name', application_name,
                'state', state,
                'sync_state', sync_state,
                'lag_bytes', pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn),
                'lag_seconds', date_part('epoch', clock_timestamp() - reply_time)
            )) AS standby_lag
            FROM pg_stat_replication),
        slots AS (
            SELECT jsonb_agg(jsonb_build_object(
                'slot_name', slot_name,
                'slot_type', slot_type,
                'active', active,
                'wal_bytes', CASE
                    WHEN pg_is_in_recovery() THEN NULL
                    ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
                END
            )) AS slot_stats
            FROM pg_replication_slots
            WHERE slot_type = 'physical'
               OR database = current_database())
    INSERT INTO @extschema@.repl (
        snapshot_tstamp,
        standby_lag,
        slot_stats)
    SELECT
        snapshot_tstamp,
        (SELECT standby_lag FROM standbys),
        (SELECT slot_stats FROM slots);
$$ LANGUAGE SQL;

-- Add SLRU stats
CREATE TABLE IF NOT EXISTS @extschema@.slru(
    snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
    slru_stats jsonb);

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
    FROM pg_stat_slru;
$$ LANGUAGE SQL;

-- Update snapshot function
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

-- Make new tables dumpable
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.repl', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.slru', '');

-- Remove duplicate config snapshots (keep first and those where config changed)
SELECT 'compacting conf table, this may take some time...' AS notice;

DELETE FROM @extschema@.conf
WHERE snapshot_tstamp NOT IN (
    SELECT snapshot_tstamp
    FROM (
        SELECT snapshot_tstamp,
               LAG(conf) OVER (ORDER BY snapshot_tstamp) AS prev_conf,
               conf
        FROM @extschema@.conf
    ) t
    WHERE prev_conf IS NULL
       OR conf IS DISTINCT FROM prev_conf
);
SELECT 'done.' AS notice;

-- Update snapshot_conf to only store when config changes
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
            'vacuum_cost_limit')) s;

    SELECT conf INTO previous_conf
    FROM @extschema@.conf
    WHERE snapshot_tstamp = (SELECT MAX(snapshot_tstamp) FROM @extschema@.conf);

    IF previous_conf IS NULL OR current_conf IS DISTINCT FROM previous_conf THEN
        INSERT INTO @extschema@.conf (snapshot_tstamp, conf)
        VALUES (snapshot_conf.snapshot_tstamp, current_conf);
    END IF;
END;
$$ LANGUAGE plpgsql;
