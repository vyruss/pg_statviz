-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo USE "ALTER EXTENSION pg_statviz UPDATE TO 1.2" TO LOAD this file. \quit
CREATE OR REPLACE FUNCTION @extschema@.snapshot_conf(snapshot_tstamp timestamptz) RETURNS void AS $$
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

-- server uptime
CREATE TABLE IF NOT EXISTS @extschema@.uptime
    (
      snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
      uptime bigint);

CREATE OR REPLACE FUNCTION @extschema@.snapshot_uptime(snapshot_tstamp timestampz) 
  RETURNS void
  AS $$
    INSERT INTO @extschema@.uptime (
      snapshot_tstamp,
      uptime)
    SELECT
      snapshot_tstamp,
      extract(epoch from current_timestamp - pg_postmaster_start_time())::bigint as uptime;
  $$ LANGUAGE SQL;

  
-- Locks
CREATE TABLE IF NOT EXISTS @extschema@.lock(
    snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
    lock_total int,
    lock_access_share int,
    lock_row_share int,
    lock_row_ex int,
    lock_share_update_ex int,
    lock_share int,
    lock_share_row_ex int,
    lock_ex int,
    lock_access_ex int);

CREATE OR REPLACE FUNCTION @extschema@.snapshot_lock(snapshot_tstamp timestampz)
RETURNS VOID
AS $$
  WITH
    pgl AS (
      SELECT mode
       FROM pg_locks)
  INSERT INTO @extschema@.lock (
    snapshot_tstamp,
    lock_total,
    lock_access_share ,
    lock_row_share ,
    lock_row_ex ,
    lock_share_update_ex ,
    lock_share ,
    lock_share_row_ex ,
    lock_ex ,
    lock_access_ex) 
  SELECT 
    snapshot_tstamp,
    count(*) AS lock_total,
    count(*) FILTER (WHERE mode = 'AccessShareLock') as lock_access_share,
    count(*) FILTER (WHERE mode = 'RowShareLock') as lock_row_share,
    count(*) FILTER (WHERE mode = 'RowExclusiveLock') as lock_row_ex,
    count(*) FILTER (WHERE mode = 'ShareUpdateExclusiveLock') as lock_share_update_ex,
    count(*) FILTER (WHERE mode = 'ShareLock') as lock_share,
    count(*) FILTER (WHERE mode = 'ShareRowExclusiveLock') as lock_share_row_ex,
    count(*) FILTER (WHERE mode = 'ExclusiveLock') as lock_ex,
    count(*) FILTER (WHERE mode = 'AccessExclusiveLock') as lock_access_ex
  FROM pgl;
$$ LANGUAGE SQL;
