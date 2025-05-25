use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.column_store_segments
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
		s.on_disk_size,
		s.collation_id,
		s.min_deep_data,
		s.max_deep_data
	FROM sys.syscscolsegments s
	INNER JOIN sys.sysrowsets p ON s.hobt_id = p.rowsetid
	INNER JOIN sys.sysrscols c ON s.hobt_id = c.rsid AND s.column_id = c.hbcolid
	WHERE HAS_ACCESS('CO', p.idmajor) = 1
	
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
		sg.on_disk_size,
		NULL,
		NULL,
		NULL
	FROM OPENROWSET(TABLE HKCS_SEGMENTS) sg
	WHERE HAS_ACCESS('CO', sg.object_id) = 1


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.column_store_segments
AS
	SELECT s.hobt_id as partition_id,
		s.hobt_id,
		c.rscolid as column_id,
		s.segment_id,
		s.version,
		s.encoding_type,
		s.row_count,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(p.idmajor) + '.' + OBJECT_NAME(p.idmajor), 'OBJECT', 'SELECT') = 1 then sysconv(bit, s.status & 1) else NULL end) as has_nulls,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(p.idmajor) + '.' + OBJECT_NAME(p.idmajor), 'OBJECT', 'SELECT') = 1 then s.base_id else NULL end) as base_id,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(p.idmajor) + '.' + OBJECT_NAME(p.idmajor), 'OBJECT', 'SELECT') = 1 then s.magnitude else NULL end) as magnitude,
		s.primary_dictionary_id,
		s.secondary_dictionary_id,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(p.idmajor) + '.' + OBJECT_NAME(p.idmajor), 'OBJECT', 'SELECT') = 1 then s.min_data_id else NULL end) as min_data_id,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(p.idmajor) + '.' + OBJECT_NAME(p.idmajor), 'OBJECT', 'SELECT') = 1 then s.max_data_id else NULL end) as max_data_id,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(p.idmajor) + '.' + OBJECT_NAME(p.idmajor), 'OBJECT', 'SELECT') = 1 then s.null_value else NULL end) as null_value,
		s.on_disk_size,
		s.collation_id,
		s.min_deep_data,
		s.max_deep_data
	FROM sys.syscscolsegments s
	INNER JOIN sys.sysrowsets p ON s.hobt_id = p.rowsetid
	INNER JOIN sys.sysrscols c ON s.hobt_id = c.rsid and s.column_id = c.hbcolid
	WHERE has_access('CO', p.idmajor) = 1	
	
	UNION ALL
	
	SELECT sg.partition_id,
		sg.hobt_id,
		sg.column_id,
		sg.segment_id,
		sg.version,
		sg.encoding_type,
		sg.row_count,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(sg.object_id) + '.' + OBJECT_NAME(sg.object_id), 'OBJECT', 'SELECT') = 1 then sg.has_nulls else NULL end) as has_nulls,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(sg.object_id) + '.' + OBJECT_NAME(sg.object_id), 'OBJECT', 'SELECT') = 1 then sg.base_id else NULL end) as base_id,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(sg.object_id) + '.' + OBJECT_NAME(sg.object_id), 'OBJECT', 'SELECT') = 1 then sg.magnitude else NULL end) as magnitude,
		sg.primary_dictionary_id,
		sg.secondary_dictionary_id,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(sg.object_id) + '.' + OBJECT_NAME(sg.object_id), 'OBJECT', 'SELECT') = 1 then sg.min_data_id else NULL end) as min_data_id,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(sg.object_id) + '.' + OBJECT_NAME(sg.object_id), 'OBJECT', 'SELECT') = 1 then sg.max_data_id else NULL end) as max_data_id,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(sg.object_id) + '.' + OBJECT_NAME(sg.object_id), 'OBJECT', 'SELECT') = 1 then sg.null_value else NULL end) as null_value,				
		sg.on_disk_size,
		NULL,
		NULL,
		NULL
	FROM OpenRowset(TABLE HKCS_SEGMENTS) sg
	WHERE has_access('CO', sg.object_id) = 1

