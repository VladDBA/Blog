use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.column_store_dictionaries
AS
	SELECT d.hobt_id AS partition_id,
		d.hobt_id,
		c.rscolid AS column_id,
		d.dictionary_id,
		d.version,
		d.type,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(p.idmajor)) + '.' + QUOTENAME(OBJECT_NAME(p.idmajor)), 'OBJECT', 'SELECT') = 1 THEN d.last_id ELSE NULL END) AS last_id,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(p.idmajor)) + '.' + QUOTENAME(OBJECT_NAME(p.idmajor)), 'OBJECT', 'SELECT') = 1 THEN d.entry_count ELSE NULL END) AS entry_count,
		d.on_disk_size
	FROM sys.syscsdictionaries d
	INNER JOIN sys.sysrowsets p ON d.hobt_id = p.rowsetid
	INNER JOIN sys.sysrscols c ON d.hobt_id = c.rsid AND d.column_id = c.hbcolid
	WHERE HAS_ACCESS('CO', p.idmajor) = 1
	
	UNION ALL

	SELECT dict.partition_id,
		dict.hobt_id,
		dict.column_id,
		dict.dictionary_id,
		dict.version,
		dict.type,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(dict.object_id)) + '.' + QUOTENAME(OBJECT_NAME(dict.object_id)), 'OBJECT', 'SELECT') = 1 THEN dict.last_id ELSE NULL END) AS last_id,
		(CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(dict.object_id)) + '.' + QUOTENAME(OBJECT_NAME(dict.object_id)), 'OBJECT', 'SELECT') = 1 THEN dict.entry_count ELSE NULL END) AS entry_count,		
		dict.on_disc_size
	FROM OPENROWSET(TABLE HKCS_DICTIONARIES) dict
	WHERE HAS_ACCESS('CO', dict.object_id) = 1


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.column_store_dictionaries
AS
	SELECT d.hobt_id as partition_id,
		d.hobt_id,
		c.rscolid as column_id,
		d.dictionary_id,
		d.version,
		d.type,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(p.idmajor) + '.' + OBJECT_NAME(p.idmajor), 'OBJECT', 'SELECT') = 1 then d.last_id else NULL end) as last_id,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(p.idmajor) + '.' + OBJECT_NAME(p.idmajor), 'OBJECT', 'SELECT') = 1 then d.entry_count else NULL end) as entry_count,
		d.on_disk_size
	FROM sys.syscsdictionaries d
	INNER JOIN sys.sysrowsets p ON d.hobt_id = p.rowsetid
	INNER JOIN sys.sysrscols c ON d.hobt_id = c.rsid and d.column_id = c.hbcolid
	WHERE has_access('CO', p.idmajor) = 1
	
	UNION ALL

	SELECT dict.partition_id,
		dict.hobt_id,
		dict.column_id,
		dict.dictionary_id,
		dict.version,
		dict.type,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(dict.object_id) + '.' + OBJECT_NAME(dict.object_id), 'OBJECT', 'SELECT') = 1 then dict.last_id else NULL end) as last_id,
		(case when has_perms_by_name(OBJECT_SCHEMA_NAME(dict.object_id) + '.' + OBJECT_NAME(dict.object_id), 'OBJECT', 'SELECT') = 1 then dict.entry_count else NULL end) as entry_count,		
		dict.on_disc_size
	FROM OpenRowset(TABLE HKCS_DICTIONARIES) dict
	WHERE has_access('CO', dict.object_id) = 1

