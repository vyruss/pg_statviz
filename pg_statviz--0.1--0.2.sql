-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_statviz UPDATE TO 1.2" to load this file. \quit

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
            'bgwriter_delay',
            'bgwriter_lru_maxpages',
            'bgwriter_lru_multiplier',
            'checkpoint_completion_target',
            'checkpoint_timeout',
            'max_connections',
            'max_wal_size',
            'max_wal_senders',
            'work_mem',
            'autovacuum_max_workers',
            'autovacuum_naptime',
            'autovacuum_work_mem',
            'maintenance_work_mem',
            'max_replication_slots',
            'max_parallel_workers',
            'server_version_num',
            'shared_buffers')) s;
$$ LANGUAGE SQL;