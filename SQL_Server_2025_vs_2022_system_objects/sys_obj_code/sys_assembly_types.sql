SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.assembly_types AS
	SELECT t.name,
		t.xtype AS system_type_id,
		t.id AS user_type_id,
		t.schid AS schema_id,
		pr.indepid AS principal_id,
		t.length AS max_length,
		t.prec AS precision,
		t.scale,
		convert(sysname, collationpropertyfromid(t.collationid, 'name')) AS collation_name,
		sysconv(bit, 1 - (t.status & 1)) AS is_nullable,	-- COL80_NOTNULL
		sysconv(bit, case when t.id > 256 then 1 else 0 end) AS is_user_defined, 	-- x_utypSSNAME
		sysconv(bit, 1) AS is_assembly_type,
		sysconv(int, 0) AS default_object_id,
		sysconv(int, 0) AS rule_object_id,
		r.indepid AS assembly_id,
		convert(sysname, v.name) collate Latin1_General_BIN AS assembly_class,
		sysconv(bit, typepropertyex(t.id,'is_binary_ordered')) AS is_binary_ordered,
		sysconv(bit, typepropertyex(t.id,'is_fixed_length')) AS is_fixed_length,
		convert(nvarchar(40), typepropertyex(t.id,'prog_id')) AS prog_id,
		convert(nvarchar(4000), typepropertyex(t.id,'assembly_qualified_name')) collate Latin1_General_BIN AS assembly_qualified_name,
		sysconv(bit, 0) AS is_table_type
	FROM sys.sysscalartypes$ t
	JOIN sys.syssingleobjrefs r ON r.depid = t.id AND r.class = 38 AND r.depsubid = 0	-- SRC_TYPETOASM
	LEFT JOIN sys.syssingleobjrefs pr ON pr.depid = t.id AND pr.class = 44 AND pr.depsubid = 0	-- SRC_TYPETOOWNER
	LEFT JOIN sys.sysbinsubobjs v ON v.class = 11 AND v.idmajor = r.indepid AND v.subid = t.id  	-- SOC_ASSEMBLY_TYPE
	WHERE has_access('UT', t.id) = 1


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.assembly_types AS
	SELECT t.name,
		t.xtype AS system_type_id,
		t.id AS user_type_id,
		t.schid AS schema_id,
		pr.indepid AS principal_id,
		t.length AS max_length,
		t.prec AS precision,
		t.scale,
		convert(sysname, collationpropertyfromid(t.collationid, 'name')) AS collation_name,
		sysconv(bit, 1 - (t.status & 1)) AS is_nullable,	-- COL80_NOTNULL
		sysconv(bit, case when t.id > 256 then 1 else 0 end) AS is_user_defined, 	-- x_utypSSNAME
		sysconv(bit, 1) AS is_assembly_type,
		sysconv(int, 0) AS default_object_id,
		sysconv(int, 0) AS rule_object_id,
		r.indepid AS assembly_id,
		convert(sysname, v.name) collate Latin1_General_BIN AS assembly_class,
		sysconv(bit, typepropertyex(t.id,'is_binary_ordered')) AS is_binary_ordered,
		sysconv(bit, typepropertyex(t.id,'is_fixed_length')) AS is_fixed_length,
		convert(nvarchar(40), typepropertyex(t.id,'prog_id')) AS prog_id,
		convert(nvarchar(4000), typepropertyex(t.id,'assembly_qualified_name')) collate Latin1_General_BIN AS assembly_qualified_name,
		sysconv(bit, 0) AS is_table_type
	FROM sys.sysscalartypes t
	JOIN sys.syssingleobjrefs r ON r.depid = t.id AND r.class = 38 AND r.depsubid = 0	-- SRC_TYPETOASM
	LEFT JOIN sys.syssingleobjrefs pr ON pr.depid = t.id AND pr.class = 44 AND pr.depsubid = 0	-- SRC_TYPETOOWNER
	LEFT JOIN sys.sysbinsubobjs v ON v.class = 11 AND v.idmajor = r.indepid AND v.subid = t.id  	-- SOC_ASSEMBLY_TYPE
	WHERE has_access('UT', t.id) = 1

