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

-- User tables
CREATE TABLE IF NOT EXISTS @extschema@.user_table (
  snapshot_tstamp timestamptz REFERENCES @extschema@.snapshots(snapshot_tstamp) ON DELETE CASCADE PRIMARY KEY,
  cnt_tables int,
  tables jsonb);


CREATE OR REPLACE FUNCTION @extschema@.snapshot_table (snapshot_tstamp timestamptz)
RETURNS VOID
AS $$
  WITH 
    pgtb AS (
      SELECT 
        a.relname as table_name,
        a.schemaname as schema_name,
        coalesce(b.heap_blks_read,0) as heap_blks_read,
        coalesce(b.heap_blks_hit,0) as heap_blks_hit,
        coalesce(b.idx_blks_read,0) as idx_blks_read,
        coalesce(b.idx_blks_hit,0) as idx_blks_hit,
        coalesce(b.toast_blks_read,0) as toast_blks_read,
        coalesce(b.toast_blks_hit,0) as toast_blks_hit,
        coalesce(a.seq_scan,0) as seq_scan,
        coalesce(a.seq_tup_read,0) as seq_tup_read,
        coalesce(a.idx_scan,0) as idx_scan,
        coalesce(a.idx_tup_fetch,0) as idx_tup_fetch,
        coalesce(a.n_tup_ins,0) as n_tup_ins,
        coalesce(a.n_tup_upd,0) as n_tup_upd,
        coalesce(a.n_tup_del,0) as n_tup_del,
        coalesce(a.n_live_tup,0) as n_live_tup,
        coalesce(a.n_dead_tup,0) as n_dead_tup,
        coalesce(a.vacuum_count,0) as vacuum_count,
        coalesce(a.autovacuum_count,0) as autovacuum_count,
        coalesce(a.analyze_count,0) as analyze_count,
        coalesce(a.autoanalyze_count,0) as autoanalyze_count,
        pg_relation_size(a.relid) as size
      FROM
        pg_stat_user_tables AS a INNER JOIN pg_statio_user_tables AS b
      ON a.relid=b.relid)
    INSERT INTO @extschema@.user_table (
      snapshot_tstamp,
      cnt_tables,
      tables)
    SELECT
      snapshot_tstamp,
      count(*),
      coalesce(jsonb_agg(pgtb),'[]'::jsonb)
    FROM pgtb;
$$ LANGUAGE SQL;

