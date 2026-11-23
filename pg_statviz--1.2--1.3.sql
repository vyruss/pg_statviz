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
DO $block$
BEGIN
    IF (SELECT current_setting('server_version_num')::int >= 190000) THEN
        CREATE OR REPLACE FUNCTION @extschema@.snapshot_lockwait(snapshot_tstamp timestamptz)
        RETURNS void
        AS $$
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
                    FROM pg_stat_lock
                    WHERE waits > 0 OR wait_time > 0 OR fastpath_exceeded > 0),
                (SELECT max(stats_reset) FROM pg_stat_lock);
        $$ LANGUAGE SQL;
    END IF;
END
$block$ LANGUAGE PLPGSQL;


-- Add lock wait statistics to the snapshot function
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
        -- pg_stat_lock only exists in PG19+
        IF (SELECT current_setting('server_version_num')::int >= 190000) THEN
            PERFORM @extschema@.snapshot_lockwait(ts);
        END IF;
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


GRANT SELECT, INSERT, DELETE, TRUNCATE ON @extschema@.lockwait TO pg_monitor;

SELECT pg_catalog.pg_extension_config_dump('pgstatviz.lockwait', '');
