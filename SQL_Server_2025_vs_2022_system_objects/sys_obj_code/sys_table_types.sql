SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.table_types AS
	SELECT t.name AS name,
		t.xtype AS system_type_id,
		t.id AS user_type_id,
		t.schid AS schema_id,
		own.indepid AS principal_id,
		t.length AS max_length,
		t.prec AS precision,
		t.scale AS scale,
		convert(sysname, collationpropertyfromid(t.collationid, 'name')) AS collation_name,
		sysconv(bit, 1 - (t.status & 1)) AS is_nullable,	-- TYPE_NOTNULL
		sysconv(bit, 1) AS is_user_defined,
		sysconv(bit, 0) AS is_assembly_type,
		sysconv(int, 0) AS default_object_id,
		sysconv(int, 0) AS rule_object_id,
		sysconv(bit, 1) AS is_table_type,
		o.indepid AS type_table_object_id,
		sysconv(bit, obj.status2 & 0x00000008) AS is_memory_optimized	-- OBJTAB2_HEKATON
	FROM sys.sysscalartypes$ t
	LEFT JOIN sys.syssingleobjrefs own ON own.depid = t.id AND own.class = 44 AND own.depsubid = 0	-- SRC_TYPETOOWNER
	INNER JOIN sys.syssingleobjrefs o ON o.depid = t.id AND o.class = 36 AND o.depsubid = 0	-- SRC_TYPETOTABLE
	INNER JOIN sys.sysschobjs$ obj ON o.indepid = obj.id
		-- 36==SRC_TYPETOTABLE, so only table types show up in this view
	WHERE has_access('UT', t.id) = 1


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.table_types AS
	SELECT t.name AS name,
		t.xtype AS system_type_id,
		t.id AS user_type_id,
		t.schid AS schema_id,
		own.indepid AS principal_id,
		t.length AS max_length,
		t.prec AS precision,
		t.scale AS scale,
		convert(sysname, collationpropertyfromid(t.collationid, 'name')) AS collation_name,
		sysconv(bit, 1 - (t.status & 1)) AS is_nullable,	-- TYPE_NOTNULL
		sysconv(bit, 1) AS is_user_defined,
		sysconv(bit, 0) AS is_assembly_type,
		sysconv(int, 0) AS default_object_id,
		sysconv(int, 0) AS rule_object_id,
		sysconv(bit, 1) AS is_table_type,
		o.indepid AS type_table_object_id,
		sysconv(bit, obj.status2 & 0x00000008) AS is_memory_optimized	-- OBJTAB2_HEKATON
	FROM sys.sysscalartypes t
	LEFT JOIN sys.syssingleobjrefs own ON own.depid = t.id AND own.class = 44 AND own.depsubid = 0	-- SRC_TYPETOOWNER
	INNER JOIN sys.syssingleobjrefs o ON o.depid = t.id AND o.class = 36 AND o.depsubid = 0	-- SRC_TYPETOTABLE
	INNER JOIN sys.sysschobjs$ obj ON o.indepid = obj.id
		-- 36==SRC_TYPETOTABLE, so only table types show up in this view
	WHERE has_access('UT', t.id) = 1

