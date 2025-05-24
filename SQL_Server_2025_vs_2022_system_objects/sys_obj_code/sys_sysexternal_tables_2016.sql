SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE VIEW sys.sysexternal_tables_2016 AS
	SELECT
		obj.name AS name,
		obj.object_id AS object_id,
		obj.principal_id AS principal_id,
		obj.schema_id AS schema_id,
		obj.parent_object_id AS parent_object_id,
		obj.type AS type,
		obj.type_desc AS type_desc,	
		obj.create_date AS create_date,
		obj.modify_date AS modify_date,
		obj.is_ms_shipped AS is_ms_shipped,	
		obj.is_published AS is_published,
		obj.is_schema_published AS is_schema_published,
		(SELECT TOP 1 column_id from sys.columns cols
			WHERE cols.object_id = et.object_id
			ORDER BY cols.column_id DESC) AS max_column_id_used,	
		obj.uses_ansi_nulls AS uses_ansi_nulls,
		et.data_source_id AS data_source_id,
		et.file_format_id AS file_format_id,
		et.location AS location,
		et.reject_type AS reject_type,
		et.reject_value AS reject_value,
		et.reject_sample_value AS reject_sample_value,
		CASE
			WHEN et.sharding_dist_type < 3 THEN et.sharding_dist_type
			ELSE NULL
		END AS distribution_type,
		CASE 
			WHEN et.sharding_dist_type = 0 THEN CONVERT(NVARCHAR(120), 'SHARDED')
			WHEN et.sharding_dist_type = 1 THEN CONVERT(NVARCHAR(120), 'REPLICATED')
			WHEN et.sharding_dist_type = 2 THEN CONVERT(NVARCHAR(120), 'ROUND_ROBIN')
			ELSE NULL
		END AS distribution_desc,
		CASE 
			WHEN et.sharding_col_id <> -1 THEN et.sharding_col_id
			ELSE NULL
		END AS sharding_col_id,
		et.source_schema_name as remote_schema_name,
		et.source_table_name as remote_object_name
	FROM sys.sysexttables et 
		INNER JOIN sys.objects$ obj on et.object_id = obj.object_id

