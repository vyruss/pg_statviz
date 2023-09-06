/*
// pg_statviz--0.5--0.6.sql - Upgrade extension to 0.6
*/

SELECT pg_catalog.pg_extension_config_dump('pgstatviz.buf', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.conf', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.conn', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.db', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.lock', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.snapshots', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.wait', '');
SELECT pg_catalog.pg_extension_config_dump('pgstatviz.wal', '');
