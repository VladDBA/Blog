SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW INFORMATION_SCHEMA.COLUMNS
AS
SELECT
	DB_NAME()									AS TABLE_CATALOG,
	SCHEMA_NAME(o.schema_id)						AS TABLE_SCHEMA,
	o.name										AS TABLE_NAME,
	c.name										AS COLUMN_NAME,
	COLUMNPROPERTY(c.object_id, c.name, 'ordinal')		AS ORDINAL_POSITION,
	convert(nvarchar(4000),
		OBJECT_DEFINITION(c.default_object_id))			AS COLUMN_DEFAULT,
	convert(varchar(3), CASE c.is_nullable
		WHEN 1 THEN 'YES' ELSE 'NO' END)			AS IS_NULLABLE,
	ISNULL(TYPE_NAME(CASE
		WHEN c.user_type_id = 255 THEN c.user_type_id -- vector
		ELSE c.system_type_id END), t.name)			AS DATA_TYPE,
	COLUMNPROPERTY(c.object_id, c.name, 'charmaxlen')	AS CHARACTER_MAXIMUM_LENGTH,
	COLUMNPROPERTY(c.object_id, c.name, 'octetmaxlen')	AS CHARACTER_OCTET_LENGTH,
	convert(tinyint, CASE -- int/decimal/numeric/real/float/money
		WHEN c.system_type_id IN (48, 52, 56, 59, 60, 62, 106, 108, 122, 127) THEN c.precision
		END)										AS NUMERIC_PRECISION,
	convert(smallint, CASE	-- int/money/decimal/numeric
		WHEN c.system_type_id IN (48, 52, 56, 60, 106, 108, 122, 127) THEN 10
		WHEN c.system_type_id IN (59, 62) THEN 2 END)	AS NUMERIC_PRECISION_RADIX,	-- real/float
	convert(int, CASE	-- datetime/smalldatetime
		WHEN c.system_type_id IN (40, 41, 42, 43, 58, 61) THEN NULL
		ELSE ODBCSCALE(c.system_type_id, c.scale) END)	AS NUMERIC_SCALE,
	convert(smallint, CASE -- datetime/smalldatetime
		WHEN c.system_type_id IN (40, 41, 42, 43, 58, 61) THEN ODBCSCALE(c.system_type_id, c.scale) END)	AS DATETIME_PRECISION,
	convert(sysname, null)					AS CHARACTER_SET_CATALOG,
	convert(sysname, null) collate catalog_default	AS CHARACTER_SET_SCHEMA,
	convert(sysname, CASE
		WHEN c.system_type_id IN (35, 167, 175)	-- char/varchar/text
			THEN COLLATIONPROPERTY(c.collation_name, 'sqlcharsetname')
		WHEN c.system_type_id IN (99, 231, 239)	-- nchar/nvarchar/ntext
			THEN N'UNICODE'
		END)						AS CHARACTER_SET_NAME,
	convert(sysname, null)				AS COLLATION_CATALOG,
	convert(sysname, null) collate catalog_default		AS COLLATION_SCHEMA,
	c.collation_name					AS COLLATION_NAME,
	convert(sysname, CASE WHEN c.user_type_id > 256
		THEN DB_NAME() END)			AS DOMAIN_CATALOG,
	convert(sysname, CASE WHEN c.user_type_id > 256
		THEN SCHEMA_NAME(t.schema_id)
		END)						AS DOMAIN_SCHEMA,
	convert(sysname, CASE WHEN c.user_type_id > 256  
		THEN TYPE_NAME(c.user_type_id)
		END)						AS DOMAIN_NAME
FROM
	sys.objects o JOIN sys.columns c ON c.object_id = o.object_id
	LEFT JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE
	o.type IN ('U', 'V')


/*====  SQL Server 2022 version  ====*/
CREATE VIEW INFORMATION_SCHEMA.COLUMNS
AS
SELECT
	DB_NAME()									AS TABLE_CATALOG,
	SCHEMA_NAME(o.schema_id)						AS TABLE_SCHEMA,
	o.name										AS TABLE_NAME,
	c.name										AS COLUMN_NAME,
	COLUMNPROPERTY(c.object_id, c.name, 'ordinal')		AS ORDINAL_POSITION,
	convert(nvarchar(4000),
		OBJECT_DEFINITION(c.default_object_id))			AS COLUMN_DEFAULT,
	convert(varchar(3), CASE c.is_nullable
		WHEN 1 THEN 'YES' ELSE 'NO' END)			AS IS_NULLABLE,
	ISNULL(TYPE_NAME(c.system_type_id), t.name)		AS DATA_TYPE,
	COLUMNPROPERTY(c.object_id, c.name, 'charmaxlen')	AS CHARACTER_MAXIMUM_LENGTH,
	COLUMNPROPERTY(c.object_id, c.name, 'octetmaxlen')	AS CHARACTER_OCTET_LENGTH,
	convert(tinyint, CASE -- int/decimal/numeric/real/float/money
		WHEN c.system_type_id IN (48, 52, 56, 59, 60, 62, 106, 108, 122, 127) THEN c.precision
		END)										AS NUMERIC_PRECISION,
	convert(smallint, CASE	-- int/money/decimal/numeric
		WHEN c.system_type_id IN (48, 52, 56, 60, 106, 108, 122, 127) THEN 10
		WHEN c.system_type_id IN (59, 62) THEN 2 END)	AS NUMERIC_PRECISION_RADIX,	-- real/float
	convert(int, CASE	-- datetime/smalldatetime
		WHEN c.system_type_id IN (40, 41, 42, 43, 58, 61) THEN NULL
		ELSE ODBCSCALE(c.system_type_id, c.scale) END)	AS NUMERIC_SCALE,
	convert(smallint, CASE -- datetime/smalldatetime
		WHEN c.system_type_id IN (40, 41, 42, 43, 58, 61) THEN ODBCSCALE(c.system_type_id, c.scale) END)	AS DATETIME_PRECISION,
	convert(sysname, null)					AS CHARACTER_SET_CATALOG,
	convert(sysname, null) collate catalog_default	AS CHARACTER_SET_SCHEMA,
	convert(sysname, CASE
		WHEN c.system_type_id IN (35, 167, 175)	-- char/varchar/text
			THEN COLLATIONPROPERTY(c.collation_name, 'sqlcharsetname')
		WHEN c.system_type_id IN (99, 231, 239)	-- nchar/nvarchar/ntext
			THEN N'UNICODE'
		END)						AS CHARACTER_SET_NAME,
	convert(sysname, null)				AS COLLATION_CATALOG,
	convert(sysname, null) collate catalog_default		AS COLLATION_SCHEMA,
	c.collation_name					AS COLLATION_NAME,
	convert(sysname, CASE WHEN c.user_type_id > 256
		THEN DB_NAME() END)			AS DOMAIN_CATALOG,
	convert(sysname, CASE WHEN c.user_type_id > 256
		THEN SCHEMA_NAME(t.schema_id)
		END)						AS DOMAIN_SCHEMA,
	convert(sysname, CASE WHEN c.user_type_id > 256  
		THEN TYPE_NAME(c.user_type_id)
		END)						AS DOMAIN_NAME
FROM
	sys.objects o JOIN sys.columns c ON c.object_id = o.object_id
	LEFT JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE
	o.type IN ('U', 'V')

