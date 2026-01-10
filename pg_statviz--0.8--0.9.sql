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
