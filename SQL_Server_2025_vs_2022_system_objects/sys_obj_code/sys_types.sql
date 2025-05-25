use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.types AS
	SELECT t.name AS name,
		t.xtype AS system_type_id,
		t.id AS user_type_id,
		t.schid AS schema_id,
		o.indepid AS principal_id,
		t.length AS max_length,
		t.prec AS precision,
		t.scale AS scale,
		convert(sysname, collationpropertyfromid(t.collationid, 'name')) AS collation_name,
		sysconv(bit, 1 - (t.status & 1)) AS is_nullable,	-- TYPE_NOTNULL
		sysconv(bit, case when t.id > 256 then 1 else 0 end) AS is_user_defined,	-- x_utypSSNAME
		sysconv(bit, case when t.xtype = 240 then 1 else 0 end) AS is_assembly_type,	-- XVT_UDT
		t.dflt AS default_object_id,
		t.chk AS rule_object_id,
		sysconv(bit, case when t.xtype = 243 then 1 else 0 end) AS is_table_type	-- XVT_TABLETYPE
	FROM sys.sysscalartypes$ t
	LEFT JOIN sys.syssingleobjrefs o ON o.depid = t.id AND o.class = 44 AND o.depsubid = 0	-- SRC_TYPETOOWNER
	LEFT JOIN sys.syssingleobjrefs sor ON sor.depid = t.id AND sor.class = 36 AND sor.depsubid = 0	-- SRC_TYPETOTABLE 
	WHERE (t.id <= 256 OR has_access('UT', t.id) = 1)


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.types AS
	SELECT t.name AS name,
		t.xtype AS system_type_id,
		t.id AS user_type_id,
		t.schid AS schema_id,
		o.indepid AS principal_id,
		t.length AS max_length,
		t.prec AS precision,
		t.scale AS scale,
		convert(sysname, collationpropertyfromid(t.collationid, 'name')) AS collation_name,
		sysconv(bit, 1 - (t.status & 1)) AS is_nullable,	-- TYPE_NOTNULL
		sysconv(bit, case when t.id > 256 then 1 else 0 end) AS is_user_defined,	-- x_utypSSNAME
		sysconv(bit, case when t.xtype = 240 then 1 else 0 end) AS is_assembly_type,	-- XVT_UDT
		t.dflt AS default_object_id,
		t.chk AS rule_object_id,
		sysconv(bit, case when t.xtype = 243 then 1 else 0 end) AS is_table_type	-- XVT_TABLETYPE
	FROM sys.sysscalartypes t
	LEFT JOIN sys.syssingleobjrefs o ON o.depid = t.id AND o.class = 44 AND o.depsubid = 0	-- SRC_TYPETOOWNER
	LEFT JOIN sys.syssingleobjrefs sor ON sor.depid = t.id AND sor.class = 36 AND sor.depsubid = 0	-- SRC_TYPETOTABLE 
	WHERE (t.id <= 256 OR has_access('UT', t.id) = 1) 

