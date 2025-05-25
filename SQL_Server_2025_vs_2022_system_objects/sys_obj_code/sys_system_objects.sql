SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.system_objects AS
	SELECT o.name,
		o.id AS object_id,
		convert(int, null) AS principal_id,
		o.nsid AS schema_id,
		convert(int, 0) AS parent_object_id,
		o.type,
		n.name AS type_desc,
		o.created AS create_date,
		o.modified AS modify_date,
		convert(bit, 1) AS is_ms_shipped,
		convert(bit, 0) AS is_published,
		convert(bit, 0) AS is_schema_published
	FROM sys.sysschobjs$ o
	LEFT JOIN sys.syspalnames n ON n.class = 'OBTY' AND n.value = o.type
	WHERE has_access('SO', o.id) = 1 AND
    (COALESCE(SERVERPROPERTY('EngineEdition'), 0) <> 11 OR o.name <> 'external_models')


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.system_objects AS
	SELECT o.name,
		o.id AS object_id,
		convert(int, null) AS principal_id,
		o.nsid AS schema_id,
		convert(int, 0) AS parent_object_id,
		o.type,
		n.name AS type_desc,
		o.created AS create_date,
		o.modified AS modify_date,
		convert(bit, 1) AS is_ms_shipped,
		convert(bit, 0) AS is_published,
		convert(bit, 0) AS is_schema_published
	FROM sys.sysschobjs$ o
	LEFT JOIN sys.syspalnames n ON n.class = 'OBTY' AND n.value = o.type
	WHERE has_access('SO', o.id) = 1

