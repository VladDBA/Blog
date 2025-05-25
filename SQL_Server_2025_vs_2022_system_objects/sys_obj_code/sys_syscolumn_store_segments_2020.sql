use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.syscolumn_store_segments_2020
AS
	SELECT s.hobt_id AS partition_id,
		s.hobt_id,
		c.rscolid AS column_id,
		s.segment_id,
		s.version,
		s.encoding_type,
		s.row_count,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(p.idmajor)) + '.' + QUOTENAME(OBJECT_NAME(p.idmajor)), 'OBJECT', 'SELECT') = 1 THEN SYSCONV(bit, s.status & 1) ELSE NULL END) AS has_nulls,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(p.idmajor)) + '.' + QUOTENAME(OBJECT_NAME(p.idmajor)), 'OBJECT', 'SELECT') = 1 THEN s.base_id ELSE NULL END) AS base_id,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(p.idmajor)) + '.' + QUOTENAME(OBJECT_NAME(p.idmajor)), 'OBJECT', 'SELECT') = 1 THEN s.magnitude ELSE NULL END) AS magnitude,
		s.primary_dictionary_id,
		s.secondary_dictionary_id,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(p.idmajor)) + '.' + QUOTENAME(OBJECT_NAME(p.idmajor)), 'OBJECT', 'SELECT') = 1 THEN s.min_data_id ELSE NULL END) AS min_data_id,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(p.idmajor)) + '.' + QUOTENAME(OBJECT_NAME(p.idmajor)), 'OBJECT', 'SELECT') = 1 THEN s.max_data_id ELSE NULL END) AS max_data_id,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(p.idmajor)) + '.' + QUOTENAME(OBJECT_NAME(p.idmajor)), 'OBJECT', 'SELECT') = 1 THEN s.null_value ELSE NULL END) AS null_value,
		s.on_disk_size
	FROM sys.syscscolsegments s
	INNER JOIN sys.sysrowsets p ON s.hobt_id = p.rowsetid
	INNER JOIN sys.sysrscols c ON s.hobt_id = c.rsid AND s.column_id = c.hbcolid
	WHERE has_access('CO', p.idmajor) = 1
	
	UNION ALL
	
	SELECT sg.partition_id,
		sg.hobt_id,
		sg.column_id,
		sg.segment_id,
		sg.version,
		sg.encoding_type,
		sg.row_count,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(sg.object_id)) + '.' + QUOTENAME(OBJECT_NAME(sg.object_id)), 'OBJECT', 'SELECT') = 1 THEN sg.has_nulls ELSE NULL END) AS has_nulls,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(sg.object_id)) + '.' + QUOTENAME(OBJECT_NAME(sg.object_id)), 'OBJECT', 'SELECT') = 1 THEN sg.base_id ELSE NULL END) AS base_id,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(sg.object_id)) + '.' + QUOTENAME(OBJECT_NAME(sg.object_id)), 'OBJECT', 'SELECT') = 1 THEN sg.magnitude ELSE NULL END) AS magnitude,
		sg.primary_dictionary_id,
		sg.secondary_dictionary_id,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(sg.object_id)) + '.' + QUOTENAME(OBJECT_NAME(sg.object_id)), 'OBJECT', 'SELECT') = 1 THEN sg.min_data_id ELSE NULL END) AS min_data_id,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(sg.object_id)) + '.' + QUOTENAME(OBJECT_NAME(sg.object_id)), 'OBJECT', 'SELECT') = 1 THEN sg.max_data_id ELSE NULL END) AS max_data_id,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(sg.object_id)) + '.' + QUOTENAME(OBJECT_NAME(sg.object_id)), 'OBJECT', 'SELECT') = 1 THEN sg.null_value ELSE NULL END) AS null_value,				
		sg.on_disk_size
	FROM OPENROWSET(TABLE HKCS_SEGMENTS) sg
	WHERE HAS_ACCESS('CO', sg.object_id) = 1

