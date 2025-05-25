use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.system_columns AS
	SELECT object_id, name collate catalog_default AS name,
		column_id, system_type_id, user_type_id,
		max_length, precision, scale,
		convert(sysname, ColumnPropertyEx(object_id, name, 'collation')) AS collation_name,
		is_nullable, is_ansi_padded, is_rowguidcol,
		is_identity, is_computed, is_filestream,
		is_replicated, is_non_sql_subscribed,
		is_merge_published, is_dts_replicated,
		is_xml_document, xml_collection_id,
		default_object_id, rule_object_id,
		is_sparse, is_column_set,
		generated_always_type,
		generated_always_type_desc,
		encryption_type, encryption_type_desc, 
		encryption_algorithm_name, column_encryption_key_id,
		column_encryption_key_database_name collate catalog_default as column_encryption_key_database_name,
		is_hidden,
		is_masked,
		graph_type,
		graph_type_desc,
		is_data_deletion_filter_column,
		ledger_view_column_type,
		ledger_view_column_type_desc,
		is_dropped_ledger_column,
		vector_dimensions,
		vector_base_type,
		vector_base_type_desc
	FROM sys.system_columns$


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.system_columns AS
	SELECT object_id, name collate catalog_default AS name,
		column_id, system_type_id, user_type_id,
		max_length, precision, scale,
		convert(sysname, ColumnPropertyEx(object_id, name, 'collation')) AS collation_name,
		is_nullable, is_ansi_padded, is_rowguidcol,
		is_identity, is_computed, is_filestream,
		is_replicated, is_non_sql_subscribed,
		is_merge_published, is_dts_replicated,
		is_xml_document, xml_collection_id,
		default_object_id, rule_object_id,
		is_sparse, is_column_set,
		generated_always_type,
		generated_always_type_desc,
		encryption_type, encryption_type_desc, 
		encryption_algorithm_name, column_encryption_key_id,
		column_encryption_key_database_name collate catalog_default as column_encryption_key_database_name,
		is_hidden,
		is_masked,
		graph_type,
		graph_type_desc,
		is_data_deletion_filter_column,
		ledger_view_column_type,
		ledger_view_column_type_desc,
		is_dropped_ledger_column
	FROM sys.system_columns$

