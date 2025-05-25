use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.columns AS
	SELECT c.id AS object_id,
		c.name, c.colid AS column_id,
		c.xtype AS system_type_id,
		c.utype AS user_type_id,
		c.length AS max_length,
		c.prec AS precision, c.scale,
		convert(sysname, CollationPropertyFromId(c.collationid, 'name')) AS collation_name,
		sysconv(bit, 1 - (c.status & 1)) AS is_nullable,		-- CPM_NOTNULL
		sysconv(bit, c.status & 2) AS is_ansi_padded,		-- CPM_NOTRIM
		sysconv(bit, c.status & 8) AS is_rowguidcol,			-- CPM_ROWGUIDCOL
		sysconv(bit, c.status & 4) AS is_identity,			-- CPM_IDENTCOL
		sysconv(bit, 
			case when sov4.value is NULL 
			then
				((c.status & 16) -- CPM_COMPUTED
				| 
				(isnull(convert(tinyint, sov7.value), 0) & 0x2)) -- CPM_FILE_METADATA_VIRTUAL_COLUMN
			else 0 end) is_computed,
		sysconv(bit, c.status & 32) AS is_filestream,			-- CPM_FILESTREAM
		sysconv(bit, c.status & 0x020000) AS is_replicated,			-- CPM_REPLICAT
		sysconv(bit, c.status & 0x040000) AS is_non_sql_subscribed,	-- CPM_NONSQSSUB
		sysconv(bit, c.status & 0x080000) AS is_merge_published,	-- CPM_MERGEREPL
		sysconv(bit, c.status & 0x100000) AS is_dts_replicated,	-- CPM_REPLDTS
		sysconv(bit, c.status & 2048) AS is_xml_document,		-- CPM_XML_DOC 
		c.xmlns AS xml_collection_id,
		c.dflt AS default_object_id,
		c.chk AS rule_object_id,
		sysconv(bit, c.status & 0x1000000) AS is_sparse,	-- CPM_SPARSE
		sysconv(bit, c.status & 0x2000000) AS is_column_set,	-- CPM_SPARSECOLUMNSET
		convert(tinyint, case when c.status & 0x10000000 = 0x10000000 then 1 when c.status & 0x20000000 = 0x20000000 then 2 when sov3.value >= 3 AND sov3.value <= 10 then sov3.value else 0 end) as generated_always_type, -- CPM_TEMPORALSYSSTARTCOLUMN CPM_TEMPORALSYSENDCOLUMN
		convert(nvarchar(60), case when c.status & 0x10000000 = 0x10000000 then 'AS_ROW_START' when c.status & 0x20000000 = 0x20000000 then 'AS_ROW_END' when sov3.value = 3 then 'AS_SUSER_SID_START' when sov3.value = 4 then 'AS_SUSER_SID_END' when sov3.value = 5 then 'AS_SUSER_SNAME_START' when sov3.value = 6 then 'AS_SUSER_SNAME_END' when sov3.value = 7 then 'AS_TRANSACTION_ID_START' when sov3.value = 8 then 'AS_TRANSACTION_ID_END' when sov3.value = 9 then 'AS_SEQUENCE_NUMBER_START' when sov3.value = 10 then 'AS_SEQUENCE_NUMBER_END' else 'NOT_APPLICABLE' end) collate Latin1_General_CI_AS_KS_WS as generated_always_type_desc, -- These _DESC columns should always be the fixed latin collation. Reason is that unlike other columns in system tables that can change depending on the collation setting of the database/instance, these are always fixed on all installations of SQL Server.
		convert(int, (convert(bigint, sov.value) / 0x100) % 0x100) as encryption_type,
		convert(nvarchar(64), v1.name) as encryption_type_desc,
		convert(sysname, v2.name) as encryption_algorithm_name,
		sor.indepid as column_encryption_key_id,
		convert(sysname, sov2.value) collate catalog_default as column_encryption_key_database_name,
		sysconv(bit, c.status & 0x2000) AS is_hidden,		-- CPM_HIDDEN
		sysconv(bit, isnull(sov5.value, 0)) AS is_masked,
		convert(int, sov4.value) as graph_type,
		convert(nvarchar(60), v3.name) collate Latin1_General_CI_AS_KS_WS as graph_type_desc,
		sysconv(bit, c.status & 0x8000) AS is_data_deletion_filter_column,		-- CPM_FILTER_COLUMN
		convert(int, sov6.value) as ledger_view_column_type,
		v4.name as ledger_view_column_type_desc,
		convert(bit, isnull(convert(tinyint, sov7.value), 0) & 0x1) as is_dropped_ledger_column, -- CPM_DROPPED_LEDGER_COLUMN
		convert(smallint, sov8.value) as vector_dimensions,
		convert(tinyint, sov9.value) as vector_base_type,
		convert(nvarchar(10), v5.name) collate Latin1_General_CI_AS_KS_WS as vector_base_type_desc
	FROM sys.syscolpars c
	LEFT JOIN sys.sysobjvalues sov ON sov.valclass = 115 /*SVC_COL_ENCRYPTION_MD*/ and sov.objid = c.id and sov.subobjid = c.colid and sov.valnum = 0 -- TYPE_AND_ALGORITHM
	LEFT JOIN sys.sysobjvalues sov2 ON sov2.valclass = 115 /*SVC_COL_ENCRYPTION_MD*/ and sov2.objid = c.id and sov2.subobjid = c.colid and sov2.valnum = 2 -- CEK_DATABASE_NAME
	LEFT JOIN sys.sysobjvalues sov3 ON sov3.valclass = 119 /*SVC_GENERATED_ALWAYS_COLUMN_TYPE*/ and sov3.objid = c.id and sov3.subobjid = c.colid
	LEFT JOIN sys.sysobjvalues sov4 ON sov4.valclass = 128 /*SVC_GRAPHDB_COLUMN_TYPE*/ and sov4.objid = c.id and sov4.subobjid = c.colid and sov4.valnum = 0
	LEFT JOIN sys.sysobjvalues sov5 ON sov5.valclass = 120 /*SVC_COL_DATA_MASK*/ and sov5.objid = c.id and sov5.subobjid = c.colid and sov5.valnum = 0
	LEFT JOIN sys.sysobjvalues sov6 ON sov6.valclass = 154 /*SVC_LEDGER_VIEW_COLUMN_TYPE*/ and sov6.objid = c.id and sov6.subobjid = c.colid and sov6.valnum = 0
	LEFT JOIN sys.sysobjvalues sov7 ON sov7.valclass = 162 /*SVC_COLUMN_STATUS_EXT*/ and sov7.objid = c.id and sov7.subobjid = c.colid and sov7.valnum = 0
	LEFT JOIN sys.sysobjvalues sov8 ON sov8.valclass = 183 /*SVC_VECTOR_COLUMN_PROPERTY*/ and sov8.objid = c.id and sov8.subobjid = c.colid and sov8.valnum = 0 -- VALNUM_VECTOR_PROPERTY_DIM_NUM
	LEFT JOIN sys.sysobjvalues sov9 ON sov9.valclass = 183 /*SVC_VECTOR_COLUMN_PROPERTY*/ and sov9.objid = c.id and sov9.subobjid = c.colid and sov9.valnum = 1 -- VALNUM_VECTOR_PROPERTY_BASE_TYPE
	LEFT JOIN sys.syssingleobjrefs sor ON sor.class = 113 /*SRC_COLUMN_TO_COL_ENCRYPTION_KEY*/ and sov.objid = sor.depid and sov.subobjid = sor.depsubid
	LEFT JOIN sys.syspalvalues v1 ON v1.class = 'CET' AND v1.value = convert(int, (convert(bigint, sov.value) / 0x100) % 0x100)
	LEFT JOIN sys.syspalvalues v2 ON v2.class = 'CEA' AND v2.value = convert(int, convert(bigint, sov.value) % 0x100)
	LEFT JOIN sys.syspalvalues v3 ON v3.class = 'EGCT' AND v3.value = convert(int, sov4.value)
	LEFT JOIN sys.syspalvalues v4 ON v4.class = 'LVCT' AND v4.value = convert(int, sov6.value)
	LEFT JOIN sys.syspalvalues v5 ON v5.class = 'NVBT' AND v5.value = convert(int, sov9.value)
	WHERE number = 0 AND has_access('CO', c.id) = 1


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.columns AS
	SELECT c.id AS object_id,
		c.name, c.colid AS column_id,
		c.xtype AS system_type_id,
		c.utype AS user_type_id,
		c.length AS max_length,
		c.prec AS precision, c.scale,
		convert(sysname, CollationPropertyFromId(c.collationid, 'name')) AS collation_name,
		sysconv(bit, 1 - (c.status & 1)) AS is_nullable,		-- CPM_NOTNULL
		sysconv(bit, c.status & 2) AS is_ansi_padded,		-- CPM_NOTRIM
		sysconv(bit, c.status & 8) AS is_rowguidcol,			-- CPM_ROWGUIDCOL
		sysconv(bit, c.status & 4) AS is_identity,			-- CPM_IDENTCOL
		sysconv(bit, 
			case when sov4.value is NULL 
			then
				((c.status & 16) -- CPM_COMPUTED
				| 
				(isnull(convert(tinyint, sov7.value), 0) & 0x2)) -- CPM_FILE_METADATA_VIRTUAL_COLUMN
			else 0 end) is_computed,
		sysconv(bit, c.status & 32) AS is_filestream,			-- CPM_FILESTREAM
		sysconv(bit, c.status & 0x020000) AS is_replicated,			-- CPM_REPLICAT
		sysconv(bit, c.status & 0x040000) AS is_non_sql_subscribed,	-- CPM_NONSQSSUB
		sysconv(bit, c.status & 0x080000) AS is_merge_published,	-- CPM_MERGEREPL
		sysconv(bit, c.status & 0x100000) AS is_dts_replicated,	-- CPM_REPLDTS
		sysconv(bit, c.status & 2048) AS is_xml_document,		-- CPM_XML_DOC 
		c.xmlns AS xml_collection_id,
		c.dflt AS default_object_id,
		c.chk AS rule_object_id,
		sysconv(bit, c.status & 0x1000000) AS is_sparse,	-- CPM_SPARSE
		sysconv(bit, c.status & 0x2000000) AS is_column_set,	-- CPM_SPARSECOLUMNSET
		convert(tinyint, case when c.status & 0x10000000 = 0x10000000 then 1 when c.status & 0x20000000 = 0x20000000 then 2 when sov3.value >= 3 AND sov3.value <= 10 then sov3.value else 0 end) as generated_always_type, -- CPM_TEMPORALSYSSTARTCOLUMN CPM_TEMPORALSYSENDCOLUMN
		convert(nvarchar(60), case when c.status & 0x10000000 = 0x10000000 then 'AS_ROW_START' when c.status & 0x20000000 = 0x20000000 then 'AS_ROW_END' when sov3.value = 3 then 'AS_SUSER_SID_START' when sov3.value = 4 then 'AS_SUSER_SID_END' when sov3.value = 5 then 'AS_SUSER_SNAME_START' when sov3.value = 6 then 'AS_SUSER_SNAME_END' when sov3.value = 7 then 'AS_TRANSACTION_ID_START' when sov3.value = 8 then 'AS_TRANSACTION_ID_END' when sov3.value = 9 then 'AS_SEQUENCE_NUMBER_START' when sov3.value = 10 then 'AS_SEQUENCE_NUMBER_END' else 'NOT_APPLICABLE' end) collate Latin1_General_CI_AS_KS_WS as generated_always_type_desc, -- These _DESC columns should always be the fixed latin collation. Reason is that unlike other columns in system tables that can change depending on the collation setting of the database/instance, these are always fixed on all installations of SQL Server.
		convert(int, (convert(bigint, sov.value) / 0x100) % 0x100) as encryption_type,
		convert(nvarchar(64), v1.name) as encryption_type_desc,
		convert(sysname, v2.name) as encryption_algorithm_name,
		sor.indepid as column_encryption_key_id,
		convert(sysname, sov2.value) collate catalog_default as column_encryption_key_database_name,
		sysconv(bit, c.status & 0x2000) AS is_hidden,		-- CPM_HIDDEN
		sysconv(bit, isnull(sov5.value, 0)) AS is_masked,
		convert(int, sov4.value) as graph_type,
		convert(nvarchar(60), v3.name) collate Latin1_General_CI_AS_KS_WS as graph_type_desc,
		sysconv(bit, c.status & 0x8000) AS is_data_deletion_filter_column,		-- CPM_FILTER_COLUMN
		convert(int, sov6.value) as ledger_view_column_type,
		v4.name as ledger_view_column_type_desc,
		convert(bit, isnull(convert(tinyint, sov7.value), 0) & 0x1) as is_dropped_ledger_column -- CPM_DROPPED_LEDGER_COLUMN
	FROM sys.syscolpars c
	LEFT JOIN sys.sysobjvalues sov ON sov.valclass = 115 /*SVC_COL_ENCRYPTION_MD*/ and sov.objid = c.id and sov.subobjid = c.colid and sov.valnum = 0 -- TYPE_AND_ALGORITHM
	LEFT JOIN sys.sysobjvalues sov2 ON sov2.valclass = 115 /*SVC_COL_ENCRYPTION_MD*/ and sov2.objid = c.id and sov2.subobjid = c.colid and sov2.valnum = 2 -- CEK_DATABASE_NAME
	LEFT JOIN sys.sysobjvalues sov3 ON sov3.valclass = 119 /*SVC_GENERATED_ALWAYS_COLUMN_TYPE*/ and sov3.objid = c.id and sov3.subobjid = c.colid
	LEFT JOIN sys.sysobjvalues sov4 ON sov4.valclass = 128 /*SVC_GRAPHDB_COLUMN_TYPE*/ and sov4.objid = c.id and sov4.subobjid = c.colid and sov4.valnum = 0
	LEFT JOIN sys.sysobjvalues sov5 ON sov5.valclass = 120 /*SVC_COL_DATA_MASK*/ and sov5.objid = c.id and sov5.subobjid = c.colid and sov5.valnum = 0
	LEFT JOIN sys.sysobjvalues sov6 ON sov6.valclass = 154 /*SVC_LEDGER_VIEW_COLUMN_TYPE*/ and sov6.objid = c.id and sov6.subobjid = c.colid and sov6.valnum = 0
	LEFT JOIN sys.sysobjvalues sov7 ON sov7.valclass = 162 /*SVC_COLUMN_STATUS_EXT*/ and sov7.objid = c.id and sov7.subobjid = c.colid and sov7.valnum = 0
	LEFT JOIN sys.syssingleobjrefs sor ON sor.class = 113 /*SRC_COLUMN_TO_COL_ENCRYPTION_KEY*/ and sov.objid = sor.depid and sov.subobjid = sor.depsubid
	LEFT JOIN sys.syspalvalues v1 ON v1.class = 'CET' AND v1.value = convert(int, (convert(bigint, sov.value) / 0x100) % 0x100)
	LEFT JOIN sys.syspalvalues v2 ON v2.class = 'CEA' AND v2.value = convert(int, convert(bigint, sov.value) % 0x100)
	LEFT JOIN sys.syspalvalues v3 ON v3.class = 'EGCT' AND v3.value = convert(int, sov4.value)
	LEFT JOIN sys.syspalvalues v4 ON v4.class = 'LVCT' AND v4.value = convert(int, sov6.value)
	WHERE number = 0 AND has_access('CO', c.id) = 1

