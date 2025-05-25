SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.parameters AS
	SELECT object_id, p.name,
		parameter_id, system_type_id,
		user_type_id, max_length,
		precision, scale,
		is_output, is_cursor_ref,
		has_default_value, is_xml_document,
		default_value, xml_collection_id,
		is_readonly, is_nullable,
		convert(int, (convert(bigint, sov.value) / 0x100) % 0x100) as encryption_type,
		convert(nvarchar(64), v1.name) as encryption_type_desc,
		convert(sysname, v2.name) as encryption_algorithm_name,
		sor.indepid as column_encryption_key_id,
		convert(sysname, sov2.value) collate catalog_default as column_encryption_key_database_name,
		convert(smallint, sov3.value) as vector_dimensions,
		convert(tinyint, sov4.value) as vector_base_type,
		convert(nvarchar(10), v3.name) collate Latin1_General_CI_AS_KS_WS as vector_base_type_desc
	FROM sys.parameters$ p
	LEFT JOIN sys.sysobjvalues sov ON sov.valclass = 121 /*SVC_PARAM_ENCRYPTION_MD*/ and sov.objid = p.object_id and sov.subobjid = p.parameter_id and sov.valnum = 0 -- TYPE_AND_ALGORITHM
	LEFT JOIN sys.sysobjvalues sov2 ON sov2.valclass = 121 /*SVC_PARAM_ENCRYPTION_MD*/ and sov2.objid = p.object_id and sov2.subobjid = p.parameter_id and sov2.valnum = 2 -- CEK_DATABASE_NAME
	LEFT JOIN sys.sysobjvalues sov3 ON sov3.valclass = 184 /*SVC_VECTOR_PARAM_PROPERTY*/ and sov3.objid = p.object_id and sov3.subobjid = p.parameter_id and sov3.valnum = 0 -- VALNUM_VECTOR_PROPERTY_DIM_NUM
	LEFT JOIN sys.sysobjvalues sov4 ON sov4.valclass = 184 /*SVC_VECTOR_PARAM_PROPERTY*/ and sov4.objid = p.object_id and sov4.subobjid = p.parameter_id and sov4.valnum = 1 -- VALNUM_VECTOR_PROPERTY_BASE_TYPE
	LEFT JOIN sys.syssingleobjrefs sor ON sor.class = 114 /*SRC_PARAM_TO_COL_ENCRYPTION_KEY*/ and sov.objid = sor.depid and sov.subobjid = sor.depsubid
	LEFT JOIN sys.syspalvalues v1 ON v1.class = 'CET' AND v1.value = convert(int, (convert(bigint, sov.value) / 0x100) % 0x100)
	LEFT JOIN sys.syspalvalues v2 ON v2.class = 'CEA' AND v2.value = convert(int, convert(bigint, sov.value) % 0x100)
	LEFT JOIN sys.syspalvalues v3 ON v3.class = 'NVBT' AND v3.value = convert(int, sov4.value)
	WHERE number = 1


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.parameters AS
	SELECT object_id, p.name,
		parameter_id, system_type_id,
		user_type_id, max_length,
		precision, scale,
		is_output, is_cursor_ref,
		has_default_value, is_xml_document,
		default_value, xml_collection_id,
		is_readonly, is_nullable,
		convert(int, (convert(bigint, sov.value) / 0x100) % 0x100) as encryption_type,
		convert(nvarchar(64), v1.name) as encryption_type_desc,
		convert(sysname, v2.name) as encryption_algorithm_name,
		sor.indepid as column_encryption_key_id,
		convert(sysname, sov2.value) collate catalog_default as column_encryption_key_database_name
	FROM sys.parameters$ p
	LEFT JOIN sys.sysobjvalues sov ON sov.valclass = 121 /*SVC_PARAM_ENCRYPTION_MD*/ and sov.objid = p.object_id and sov.subobjid = p.parameter_id and sov.valnum = 0 -- TYPE_AND_ALGORITHM
	LEFT JOIN sys.sysobjvalues sov2 ON sov2.valclass = 121 /*SVC_PARAM_ENCRYPTION_MD*/ and sov2.objid = p.object_id and sov2.subobjid = p.parameter_id and sov2.valnum = 2 -- CEK_DATABASE_NAME
	LEFT JOIN sys.syssingleobjrefs sor ON sor.class = 114 /*SRC_PARAM_TO_COL_ENCRYPTION_KEY*/ and sov.objid = sor.depid and sov.subobjid = sor.depsubid
	LEFT JOIN sys.syspalvalues v1 ON v1.class = 'CET' AND v1.value = convert(int, (convert(bigint, sov.value) / 0x100) % 0x100)
	LEFT JOIN sys.syspalvalues v2 ON v2.class = 'CEA' AND v2.value = convert(int, convert(bigint, sov.value) % 0x100)
	WHERE number = 1

