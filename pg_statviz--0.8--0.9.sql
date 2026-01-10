/*
// pg_statviz--0.8--0.9.sql - Upgrade extension to 0.9
*/

-- Add checksum failure tracking to db table
ALTER TABLE @extschema@.db ADD COLUMN checksum_failures bigint;
ALTER TABLE @extschema@.db ADD COLUMN checksum_last_failure timestamptz;

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
