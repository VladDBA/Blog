SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.filegroups AS
	SELECT o.name AS name,
		o.id AS data_space_id,
		o.type,
		c.name AS type_desc,
		sysconv(bit, o.status & 0x1) AS is_default,
		sysconv(bit, o.status & 0x4) AS is_system,
		g.guid AS filegroup_guid,
		r.indepid AS log_filegroup_id,
		sysconv(bit, o.status & 0x2) AS is_read_only,
		sysconv(bit, o.status & 0x8) AS is_autogrow_all_files
	FROM sys.sysclsobjs o
	LEFT JOIN sys.sysguidrefs g ON g.id = o.id AND g.class = 3 AND g.subid = 0 -- GRC_FGGUID
	LEFT JOIN sys.syssingleobjrefs r ON r.depid = o.id AND r.class = 58 AND r.depsubid = 0 -- SRC_FGPAIR
	LEFT JOIN sys.syspalnames c ON c.class = 'DSTY' AND c.value = o.type
	WHERE o.class = 31 AND (o.type IN ('FD', 'FG', 'FL') OR (o.type = 'FX' AND serverproperty('EngineEdition') NOT IN (5, 12)))


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.filegroups AS
	SELECT o.name AS name,
		o.id AS data_space_id,
		o.type,
		c.name AS type_desc,
		sysconv(bit, o.status & 0x1) AS is_default,
		sysconv(bit, o.status & 0x4) AS is_system,
		g.guid AS filegroup_guid,
		r.indepid AS log_filegroup_id,
		sysconv(bit, o.status & 0x2) AS is_read_only,
		sysconv(bit, o.status & 0x8) AS is_autogrow_all_files
	FROM sys.sysclsobjs o
	LEFT JOIN sys.sysguidrefs g ON g.id = o.id AND g.class = 3 AND g.subid = 0 -- GRC_FGGUID
	LEFT JOIN sys.syssingleobjrefs r ON r.depid = o.id AND r.class = 58 AND r.depsubid = 0 -- SRC_FGPAIR
	LEFT JOIN sys.syspalnames c ON c.class = 'DSTY' AND c.value = o.type
	WHERE o.class = 31 AND (o.type IN ('FD', 'FG', 'FL') OR (o.type = 'FX' AND serverproperty('EngineEdition') <> 5))

