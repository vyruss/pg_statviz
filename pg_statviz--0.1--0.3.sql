/*
// pg_statviz - stats visualization and time series analysis
//
// Copyright (c) 2023 Jimmy Angelakos
// This software is released under the PostgreSQL Licence
//
// pg_statviz--0.1--0.3.sql - Upgrade extension to 0.3
*/

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_statviz UPDATE" to load this file. \quit


CREATE OR REPLACE FUNCTION @extschema@.snapshot_conf(snapshot_tstamp timestamptz)
RETURNS void
AS $$
    INSERT INTO @extschema@.conf (
        snapshot_tstamp,
        conf)
    SELECT
        snapshot_tstamp,
        jsonb_agg(s)
    FROM (
        SELECT "name" AS "setting",
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


-- DB
ALTER TABLE @extschema@.db ADD COLUMN postmaster_start_time timestamptz;

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
            pg_postmaster_start_time()
        FROM pg_stat_database
        WHERE datname = current_database();
$$ LANGUAGE SQL;


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
        PERFORM @extschema@.snapshot_lock(ts);
        PERFORM @extschema@.snapshot_wait(ts);
        -- pg_stat_wal only exists in PG15+
        IF (SELECT current_setting('server_version_num')::int >= 150000) THEN
            PERFORM @extschema@.snapshot_wal(ts);
        END IF;
        RAISE NOTICE 'created pg_statviz snapshot';
        RETURN ts;
    END
$$ LANGUAGE PLPGSQL;


GRANT TRUNCATE ON ALL TABLES IN SCHEMA @extschema@ TO pg_monitor;
